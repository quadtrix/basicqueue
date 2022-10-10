//Package basicqueue implements very simple unicast and broadcast message queueing between goroutines
package basicqueue

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"time"

	"github.com/google/uuid"
	"github.com/quadtrix/aesengine"
	"github.com/quadtrix/servicelogger"
)

type BasicQueueType int

const (
	BQT_UNICAST      BasicQueueType = 1
	BQT_BROADCAST    BasicQueueType = 2
	BQT_SUBSCRIPTION BasicQueueType = 3
)

type JSonQueueMessage struct {
	MessageID   string
	Source      string
	Destination string
	MessageType string
	Payload     string
	Sent        string
	Expires     bool
	Expiration  time.Time
	PopOnRead   bool
}

type QueueMessage struct {
	messageID  string
	message    string
	expires    bool
	expiration time.Time
	popOnRead  bool
}

type QueueMessages struct {
	encrypted bool
	msgCount  int
	messages  []QueueMessage
}

type QueueProducer struct {
	identifier string
	lastping   time.Time
}

type QueueProducers struct {
	producerCount int
	producers     []QueueProducer
}

type QueueConsumer struct {
	identifier string
	lastping   time.Time
}

type QueueConsumers struct {
	consumerCount int
	consumers     []QueueConsumer
}

type BasicQueue struct {
	qtype             BasicQueueType
	qname             string
	msgExpire         bool
	defaultExpiryTime time.Duration
	messages          QueueMessages
	slog              *servicelogger.Logger
	producers         QueueProducers
	consumers         QueueConsumers
	AESengine         aesengine.AESEngine
	isJsonQueue       bool
	locked            bool
}

func (bq BasicQueue) producerExists(identifier string) bool {
	for _, producer := range bq.producers.producers {
		if producer.identifier == identifier {
			return true
		}
	}
	return false
}

func (bq BasicQueue) consumerExists(identifier string) bool {
	for _, consumer := range bq.consumers.consumers {
		if consumer.identifier == identifier {
			return true
		}
	}
	return false
}

func (bq *BasicQueue) RegisterConsumer(identifier string) (err error) {
	if bq.consumerExists(identifier) {
		return errors.New("duplicate consumer identifier")
	}
	consumer := QueueConsumer{
		identifier: identifier,
		lastping:   time.Now(),
	}
	bq.consumers.consumerCount++
	bq.consumers.consumers = append(bq.consumers.consumers, consumer)
	bq.slog.LogTrace(fmt.Sprintf("addConsumer.%s", bq.qname), "basicqueue", fmt.Sprintf("Registered consumer %s, queue %s now has %d consumers", identifier, bq.qname, bq.consumers.consumerCount))
	return nil
}

func (bq *BasicQueue) RegisterProducer(identifier string) (err error) {
	if bq.producerExists(identifier) {
		return errors.New("duplicate producer identifier")
	}
	producer := QueueProducer{
		identifier: identifier,
		lastping:   time.Now(),
	}
	bq.producers.producerCount++
	bq.producers.producers = append(bq.producers.producers, producer)
	bq.slog.LogTrace(fmt.Sprintf("addProducer.%s", bq.qname), "basicqueue", fmt.Sprintf("Registered producer %s, queue %s now has %d producers", identifier, bq.qname, bq.producers.producerCount))
	return nil
}

func NewQueue(slog *servicelogger.Logger, qtype BasicQueueType, qname string, expiration bool, defaultExpirationTime time.Duration) (bq *BasicQueue, err error) {
	bql := BasicQueue{
		slog:        slog,
		qtype:       qtype,
		qname:       qname,
		isJsonQueue: false,
		messages: QueueMessages{
			encrypted: false,
			msgCount:  0,
			messages:  []QueueMessage{},
		},
	}
	if bql.qtype == BQT_BROADCAST || bql.qtype == BQT_SUBSCRIPTION {
		bql.msgExpire = true
	} else {
		bql.msgExpire = expiration
	}
	bql.messages.msgCount = 0
	if bql.msgExpire {
		if defaultExpirationTime > 0 {
			bql.defaultExpiryTime = defaultExpirationTime
		} else {
			bql.defaultExpiryTime = time.Hour
		}
	} else {
		bql.defaultExpiryTime = 0
	}
	var typename string
	switch bql.qtype {
	case BQT_BROADCAST:
		typename = "Broadcast"
	case BQT_UNICAST:
		typename = "Unicast"
	case BQT_SUBSCRIPTION:
		typename = "Subscription"
	}
	bql.slog.LogTrace(fmt.Sprintf("New.%s", bql.qname), "basiqueue", fmt.Sprintf("Created queue of type %s", typename))
	go bql.loopExpiryCheck()
	return &bql, nil
}

func NewEncryptedQueue(slog *servicelogger.Logger, qtype BasicQueueType, qname string, expiration bool, defaultExpirationTime time.Duration) (bq *BasicQueue, err error) {
	bq, err = NewQueue(slog, qtype, qname, expiration, defaultExpirationTime)
	if err != nil {
		return bq, err
	}
	bq.slog.LogInfo(fmt.Sprintf("NewEncryptedQueue.%s", bq.qname), "basicqueue", "Adding AES encryption engine")
	bq.messages.encrypted = true
	bq.AESengine, err = aesengine.New()
	if err != nil {
		return bq, err
	}
	return bq, nil
}

func NewJsonQueue(slog *servicelogger.Logger, qtype BasicQueueType, qname string, expiration bool, defaultExpirationTime time.Duration) (bq *BasicQueue, err error) {
	bq, err = NewQueue(slog, qtype, qname, expiration, defaultExpirationTime)
	if err != nil {
		return bq, err
	}
	bq.isJsonQueue = true
	return bq, nil
}

func NewEncryptedJsonQueue(slog *servicelogger.Logger, qtype BasicQueueType, qname string, expiration bool, defaultExpirationTime time.Duration) (bq *BasicQueue, err error) {
	bq, err = NewJsonQueue(slog, qtype, qname, expiration, defaultExpirationTime)
	if err != nil {
		return bq, err
	}
	bq.slog.LogInfo(fmt.Sprintf("NewEncryptedJsonQueue.%s", bq.qname), "basicqueue", "Adding AES encryption engine")
	bq.messages.encrypted = true
	bq.AESengine, err = aesengine.New()
	if err != nil {
		return bq, err
	}
	return bq, nil
}

func (bq *BasicQueue) AddMessage(identifier string, messagetext string) (err error) {
	if bq.locked {
		bq.slog.LogError(fmt.Sprintf("AddMessage.%s", bq.qname), "basicqueue", "Queue is locked")
		return errors.New("queue is locked")
	}
	if !bq.producerExists(identifier) {
		bq.slog.LogWarn(fmt.Sprintf("AddMessage.%s", bq.qname), "basicqueue", fmt.Sprintf("Rejecting message from %s, not a registered producer", identifier))
		return errors.New("not a registered producer")
	}
	if bq.isJsonQueue {
		bq.slog.LogWarn(fmt.Sprintf("AddMessage.%s", bq.qname), "basicqueue", fmt.Sprintf("Rejecting message from %s. This is a JSON queue. Use method AddJsonMessage instead", identifier))
		return errors.New("incorrect method, use AddJsonMessage for JSON queues")
	}
	var exptime time.Time
	if bq.msgExpire {
		exptime = time.Now().Add(bq.defaultExpiryTime)
	}
	var popit bool
	if bq.qtype == BQT_UNICAST {
		popit = true
	} else {
		popit = false
	}
	if bq.messages.encrypted {
		encBytes := bq.AESengine.Encrypt([]byte(messagetext))
		messagetext = base64.StdEncoding.EncodeToString(encBytes)
	}
	msg := QueueMessage{
		messageID:  uuid.New().String(),
		message:    messagetext,
		expires:    bq.msgExpire,
		expiration: exptime,
		popOnRead:  popit,
	}
	bq.messages.messages = append(bq.messages.messages, msg)
	bq.messages.msgCount++
	bq.slog.LogTrace(fmt.Sprintf("AddMessage.%s", bq.qname), "basicqueue", fmt.Sprintf("Adding message at position %d with ID %s: %s", bq.messages.msgCount-1, msg.messageID, msg.message))
	return nil
}

func (bq *BasicQueue) AddJsonMessage(identifier string, source string, destination string, msgtype string, messagetext string) (err error) {
	if bq.locked {
		bq.slog.LogError(fmt.Sprintf("AddJsonMessage.%s", bq.qname), "basicqueue", "Queue is locked")
		return errors.New("queue is locked")
	}
	if !bq.producerExists(identifier) {
		bq.slog.LogWarn(fmt.Sprintf("AddJsonMessage.%s", bq.qname), "basicqueue", fmt.Sprintf("Rejecting message from %s (%s), not a registered producer", source, identifier))
		return errors.New("not a registered producer")
	}
	if !bq.isJsonQueue {
		bq.slog.LogWarn(fmt.Sprintf("AddJsonMessage.%s", bq.qname), "basicqueue", fmt.Sprintf("Rejecting message from %s (%s). This is not a JSON queue. Use method AddMessage instead", source, identifier))
		return errors.New("incorrect method, use AddMessage")
	}
	jms := JSonQueueMessage{
		MessageID:   uuid.New().String(),
		Source:      source,
		Destination: destination,
		MessageType: msgtype,
		Payload:     "",
		Sent:        time.Now().Format("2006-01-02 15:04:05"),
		Expires:     bq.msgExpire,
		PopOnRead:   false,
	}

	if jms.Expires {
		exptime := time.Now().Add(bq.defaultExpiryTime)
		jms.Expiration = exptime
	}
	if bq.qtype == BQT_UNICAST {
		jms.PopOnRead = true
	}
	if bq.messages.encrypted {
		jms.Payload = base64.StdEncoding.EncodeToString(bq.AESengine.Encrypt([]byte(messagetext)))
	} else {
		jms.Payload = messagetext
	}
	marshaled, err := json.Marshal(jms)
	if err != nil {
		return err
	}
	msg := QueueMessage{
		messageID:  jms.MessageID,
		message:    string(marshaled),
		expires:    jms.Expires,
		expiration: jms.Expiration,
		popOnRead:  jms.PopOnRead,
	}
	bq.messages.messages = append(bq.messages.messages, msg)
	bq.messages.msgCount++
	bq.slog.LogTrace(fmt.Sprintf("AddJsonMessage.%s", bq.qname), "basicqueue", fmt.Sprintf("Adding message from %s to %s (type %s) at position %d with ID %s", jms.Source, jms.Destination, jms.MessageType, bq.messages.msgCount-1, msg.messageID))
	return nil
}

func (bq *BasicQueue) Poll(identifier string) bool {
	if !bq.consumerExists(identifier) {
		bq.slog.LogWarn(fmt.Sprintf("Poll.%s", bq.qname), "basicqueue", fmt.Sprintf("Rejecting poll from %s, not a registered consumer", identifier))
		return false
	}
	if bq.messages.msgCount > 0 {
		return true
	}
	return false
}

func (bq BasicQueue) isInHistory(messageID string, messageIDHistory []string) bool {
	for _, mid := range messageIDHistory {
		if mid == messageID {
			return true
		}
	}
	return false
}

func (bq *BasicQueue) PollWithHistory(identifier string, messageIDHistory []string) bool {
	if !bq.consumerExists(identifier) {
		bq.slog.LogWarn(fmt.Sprintf("PollWithHistory.%s", bq.qname), "basicqueue", fmt.Sprintf("Rejecting poll from %s, not a registered consumer", identifier))
		return false
	}
	for _, message := range bq.messages.messages {
		if !bq.isInHistory(message.messageID, messageIDHistory) {
			return true
		}
	}
	return false
}

func (bq *BasicQueue) removeMessage(index int) {
	if bq.messages.msgCount > index {
		bq.slog.LogTrace(fmt.Sprintf("removeMessage.%s", bq.qname), "basicqueue", fmt.Sprintf("Removing message %s (%d)", bq.messages.messages[index].messageID, index))
		if index == 0 {
			if bq.messages.msgCount > 1 {
				bq.messages.messages = bq.messages.messages[1:]
			} else {
				bq.messages.messages = []QueueMessage{}
			}
			bq.messages.msgCount = len(bq.messages.messages)
			return
		}
		if index == bq.messages.msgCount-1 {
			bq.messages.messages = bq.messages.messages[:index-1]
		}
		if 0 < index && index < bq.messages.msgCount-1 {
			bq.messages.messages = append(bq.messages.messages[:index-1], bq.messages.messages[index+1:]...)
		}
		bq.messages.msgCount = len(bq.messages.messages)
	}
}

func (bq *BasicQueue) loopExpiryCheck() {
	bq.slog.LogTrace(fmt.Sprintf("loopExpiryCheck.%s", bq.qname), "basicqueue", fmt.Sprintf("Initializing message expiry checker (default %.1f minutes)", bq.defaultExpiryTime.Minutes()))
	for {
		time.Sleep(5 * time.Second)
		bq.slog.LogTrace(fmt.Sprintf("loopExpiryCheck.%s", bq.qname), "basicqueue", fmt.Sprintf("Running expiry check for %d messages", bq.messages.msgCount))
		//bq.slog.LogTrace(fmt.Sprintf("loopExpiryCheck.%s", bq.qname), "basicqueue", fmt.Sprintf("Queue struct: %v", bq))
		bq.checkForExpiry()
	}
}

func (bq *BasicQueue) checkForExpiry() {
	if bq.messages.msgCount > 0 {
		bq.locked = true
		//bq.slog.LogTrace(fmt.Sprintf("checkForExpiry.%s", bq.qname), "basicqueue", fmt.Sprintf("Starting message expiration check for queue %s", bq.qname))
		for i := bq.messages.msgCount - 1; i >= 0; i-- {
			if bq.messages.messages[i].expires {
				//bq.slog.LogTrace(fmt.Sprintf("checkForExpiry.%s", bq.qname), "basicqueue", fmt.Sprintf("Message %s expires at %s", bq.messages.messages[i].messageID, bq.messages.messages[i].expiration.Format("2006-01-02 15:04:05")))
				if time.Now().After(bq.messages.messages[i].expiration) {
					// Expired message, pop it from the queue
					//bq.slog.LogTrace(fmt.Sprintf("checkForExpiry.%s", bq.qname), "basicqueue", fmt.Sprintf("Message %s (%d) has expired. Removing it from queue %s", bq.messages.messages[i].messageID, i, bq.qname))
					bq.removeMessage(i)
				}
			}
		}
		bq.locked = false
	}
	bq.slog.LogTrace(fmt.Sprintf("checkForExpiry.%s", bq.qname), "basicqueue", "No messages to check")
}

func (bq *BasicQueue) readFirstMessage() string {
	var msgtext string
	if bq.messages.msgCount > 0 {
		bq.slog.LogTrace(fmt.Sprintf("readFirstMessage.%s", bq.qname), "basicqueue", "Reading message 0")
		msgtext = bq.messages.messages[0].message
		msgID := bq.messages.messages[0].messageID
		if bq.messages.messages[0].popOnRead {
			bq.removeMessage(0)
		}
		if bq.messages.encrypted {
			b64Decoded, err := base64.StdEncoding.DecodeString(msgtext)
			if err != nil {
				bq.slog.LogError(fmt.Sprintf("readFirstMessage.%s", bq.qname), "basicqueue", fmt.Sprintf("Failed to decode message %s", msgID))
			}
			decBytes, err := bq.AESengine.Decrypt(b64Decoded)
			if err != nil {
				bq.slog.LogError(fmt.Sprintf("readFirstMessage.%s", bq.qname), "basicqueue", fmt.Sprintf("Failed to decrypt message %s", msgID))
			}
			msgtext = string(decBytes)
		}
		return msgtext
	}
	return ""
}

func (bq *BasicQueue) readFirstJsonMessage() (jqm JSonQueueMessage, err error) {
	if bq.messages.msgCount > 0 {
		bq.slog.LogTrace(fmt.Sprintf("readFirstJsonMessage.%s", bq.qname), "basicqueue", "Reading message 0")
		err = json.Unmarshal([]byte(bq.messages.messages[0].message), &jqm)
		if err != nil {
			bq.slog.LogError(fmt.Sprintf("readFirstJsonMessage.%s", bq.qname), "basicqueue", fmt.Sprintf("Unable to unmarshal message %s", bq.messages.messages[0].messageID))
			return jqm, err
		}
		if bq.messages.messages[0].popOnRead {
			bq.removeMessage(0)
		}
		if bq.messages.encrypted {
			b64Decoded, err := base64.StdEncoding.DecodeString(jqm.Payload)
			if err != nil {
				bq.slog.LogError(fmt.Sprintf("readFirstJsonMessage.%s", bq.qname), "basicqueue", fmt.Sprintf("Failed to decode message %s", jqm.MessageID))
				return jqm, err
			}
			decBytes, err := bq.AESengine.Decrypt(b64Decoded)
			if err != nil {
				bq.slog.LogError(fmt.Sprintf("readFirstJsonMessage.%s", bq.qname), "basicqueue", fmt.Sprintf("Failed to decrypt message %s", jqm.MessageID))
				return jqm, err
			}
			jqm.Payload = string(decBytes)
		}
		return jqm, err
	}
	return jqm, errors.New("no messages in queue")
}

func (bq *BasicQueue) readSpecificJsonMessage(index int) (jqm JSonQueueMessage, err error) {
	if bq.messages.msgCount > index {
		bq.slog.LogTrace(fmt.Sprintf("readSpecificJsonMessage.%s", bq.qname), "basicqueue", fmt.Sprintf("Reading message %d", index))
		err := json.Unmarshal([]byte(bq.messages.messages[index].message), &jqm)
		if err != nil {
			bq.slog.LogError(fmt.Sprintf("readSpecificJsonMessage.%s", bq.qname), "basicqueue", fmt.Sprintf("Unable to unmarshal message %s", bq.messages.messages[index].messageID))
			return jqm, err
		}
		if bq.messages.messages[index].popOnRead {
			bq.removeMessage(index)
		}
		if bq.messages.encrypted {
			b64Decoded, err := base64.StdEncoding.DecodeString(jqm.Payload)
			if err != nil {
				bq.slog.LogError(fmt.Sprintf("readFirstJsonMessage.%s", bq.qname), "basicqueue", fmt.Sprintf("Failed to decode message %s", jqm.MessageID))
			}
			decBytes, err := bq.AESengine.Decrypt(b64Decoded)
			if err != nil {
				bq.slog.LogError(fmt.Sprintf("readFirstJsonMessage.%s", bq.qname), "basicqueue", fmt.Sprintf("Failed to decrypt message %s", jqm.MessageID))
			}
			jqm.Payload = string(decBytes)
		}
		return jqm, err
	}
	return jqm, errors.New(fmt.Sprintf("message index out of bounds [%d] with size [%d]", index, bq.messages.msgCount))
}

func (bq *BasicQueue) readSpecificMessage(index int) (msgtext string, msgid string, err error) {
	if bq.messages.msgCount > index {
		bq.slog.LogTrace(fmt.Sprintf("readSpecificMessage.%s", bq.qname), "basicqueue", fmt.Sprintf("Reading message %d", index))
		msgtext = bq.messages.messages[index].message
		msgid = bq.messages.messages[index].messageID
		if bq.messages.messages[index].popOnRead {
			bq.removeMessage(index)
		}
		if bq.messages.encrypted {
			b64Decoded, err := base64.StdEncoding.DecodeString(msgtext)
			if err != nil {
				bq.slog.LogError(fmt.Sprintf("readFirstMessage.%s", bq.qname), "basicqueue", fmt.Sprintf("Failed to decode message %s", msgid))
			}
			decBytes, err := bq.AESengine.Decrypt(b64Decoded)
			if err != nil {
				bq.slog.LogError(fmt.Sprintf("readFirstMessage.%s", bq.qname), "basicqueue", fmt.Sprintf("Failed to decrypt message %s", msgid))
			}
			msgtext = string(decBytes)
		}
		return msgtext, msgid, nil
	}
	return "", "", errors.New(fmt.Sprintf("basicqueue.readSpecificMessage.%s: queue index out of bounds %d>%d", bq.qname, index, bq.messages.msgCount))
}

func (bq *BasicQueue) Read(identifier string) (msg string, err error) {
	if !bq.consumerExists(identifier) {
		bq.slog.LogWarn(fmt.Sprintf("Read.%s", bq.qname), "basicqueue", fmt.Sprintf("Rejecting read from %s, not a registered consumer", identifier))
	}
	if bq.messages.msgCount == 0 {
		return "", errors.New(fmt.Sprintf("[Read.%s] nl.quadtrix.delta.basicqueue no messages in queue", bq.qname))
	}
	return bq.readFirstMessage(), nil
}

func (bq *BasicQueue) ReadJson(identifier string) (jqm JSonQueueMessage, err error) {
	if !bq.consumerExists(identifier) {
		bq.slog.LogWarn(fmt.Sprintf("ReadJson.%s", bq.qname), "basicqueue", fmt.Sprintf("Rejecting read from %s, not a registered consumer", identifier))
	}
	if bq.messages.msgCount == 0 {
		return jqm, errors.New("no messages in queue")
	}
	return bq.readFirstJsonMessage()
}

func (bq *BasicQueue) ReadJsonWithHistory(identifier string, messageIDHistory []string) (jqm JSonQueueMessage, err error) {
	if !bq.consumerExists(identifier) {
		bq.slog.LogWarn(fmt.Sprintf("ReadJsonWithHistory.%s", bq.qname), "basicqueue", fmt.Sprintf("Rejecting read from %s, not a registered consumer", identifier))
	}
	if bq.messages.msgCount == 0 {
		return jqm, errors.New("no messages in queue")
	}
	for index, message := range bq.messages.messages {
		if !bq.isInHistory(message.messageID, messageIDHistory) {
			return bq.readSpecificJsonMessage(index)
		}
	}
	return jqm, errors.New("No unread messages in queue")
}

func (bq *BasicQueue) ReadWithHistory(identifier string, messageIDHistory []string) (msg string, msgid string, err error) {
	if !bq.consumerExists(identifier) {
		bq.slog.LogWarn(fmt.Sprintf("ReadWithHistory.%s", bq.qname), "basicqueue", fmt.Sprintf("Rejecting read from %s, not a registered consumer", identifier))
	}
	if bq.messages.msgCount == 0 {
		return "", "", errors.New(fmt.Sprintf("[ReadWithHistory.%s] nl.quadtrix.delta.basicqueue no messages in queue", bq.qname))
	}
	for index, message := range bq.messages.messages {
		if !bq.isInHistory(message.messageID, messageIDHistory) {
			return bq.readSpecificMessage(index)
		}
	}
	return "", "", errors.New(fmt.Sprintf("basicqueue.ReadWithHistory.%s: No unread messages in queue", bq.qname))
}

func (bq BasicQueue) QStats(identifier string) (msgcount int, numproducers int, numconsumers int, encrypted bool, err error) {
	if !bq.producerExists(identifier) && !bq.consumerExists(identifier) {
		bq.slog.LogWarn(fmt.Sprintf("Qstats.%s", bq.qname), "basicqueue", fmt.Sprintf("Rejecting queue stats from %s, not a registered producer or consumer", identifier))
		return -1, -1, -1, false, errors.New("only registered producers and consumers may request queue statistics")
	}
	return bq.messages.msgCount, bq.producers.producerCount, bq.consumers.consumerCount, bq.messages.encrypted, nil
}

func (bq *BasicQueue) UnmarshalMessage(marshaled string) (unmarshaled JSonQueueMessage, err error) {
	err = json.Unmarshal([]byte(marshaled), &unmarshaled)
	return unmarshaled, err
}

func (bq BasicQueue) GetName() string {
	return bq.qname
}
