package aliyunMQS

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	//"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

type MQS struct {
	AccessKey    string
	AccessSecret string
	ContentType  string
	MqsHeaders   string
	QueueOwnId   string
	MqsUrl       string
}

//消息队列
type Queue struct {
	MQS
}

// 消息
type Message struct {
	MQS
}

// @Title 创建一个Mqs
// @Param accesskey string
// @Param accesssecret string
// @Param queueownid string
// @Param mqsurl string
func (this *MQS) NewMQS(accesskey, accesssecret, queueownid, mqsurl string) *MQS {
	this.AccessKey = accesskey
	this.AccessSecret = accesssecret
	this.QueueOwnId = queueownid
	this.MqsUrl = mqsurl
	this.ContentType = "text/xml;utf-8"
	this.MqsHeaders = "2014-07-08"
	return this
}

// @Title 获得GMT格式的时间，如：Thu, 17 Mar 2012 18:49:58 GMT
func (this *MQS) getGMTDate() string {
	return strings.Replace(time.Now().UTC().Format(time.RFC1123), "UTC", "GMT", -1)
}

func (this *MQS) getMd5(str []byte) string {
	md5Ctx := md5.New()
	md5Ctx.Write(str)
	return hex.EncodeToString(md5Ctx.Sum(nil))
}

func (this *MQS) getBase64(str []byte) string {
	return base64.StdEncoding.EncodeToString(str)
}

func (this *MQS) toXml(v interface{}) ([]byte, error) {
	if ouput, err := xml.Marshal(v); err != nil {
		return []byte(""), err
	} else {
		return ouput, nil
	}
}

// @Title 发起http请求
// @Param verb HTTP的Method(POST/PUT/GET/DELETE)
// @Param request_uri 请求地址
// @Param header http头
// @Param content_body http body
func (this *MQS) httpClient(verb, request_uri string, headers map[string]string, content_body string) (string, error) {
	client := &http.Client{}
	request, err := http.NewRequest(verb, request_uri, strings.NewReader(content_body))
	if err != nil {
		return "NewRequest失败", err
	}
	for k, v := range headers {
		request.Header.Set(k, v)
	}
	response, err := client.Do(request)
	defer response.Body.Close()
	if err != nil {
		return "client请求失败", err
	}

	content, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "获取response.Body失败", err
	}

	if response.StatusCode/100 > 1 && response.StatusCode/100 < 4 {
		return string(content), nil
	} else {
		return string(content), errors.New(fmt.Sprintf("Code:%d,Content:%s", response.StatusCode, string(content)))
	}
}

// @生产签名
// @Param verb 			HTTP的Method(POST/PUT/GET/DELETE)
// @Param content_md5 	请求内容数据的MD5值
// @Param content_type 	text/xml;charset=utf-8(默认)
// @Param gmt_date 	 	只支持GMT格式,如果请求时间和 MQS 服务器时间相差超过 15 分钟,MQS 会判定此请求不合法,返 回 400 错误
// @param CanonicalizedResource		http所请求资源的URI(统一资源标识 符)
// @Param CanonicalizedMQSHeaders	http中的x-mqs-开始的字段组合
func (this *MQS) getSignature(verb, content_md5, content_type, gmt_date, CanonicalizedResource string, CanonicalizedMQSHeaders map[string]string) string {
	keys := make([]string, len(CanonicalizedMQSHeaders))
	i := 0
	for k, _ := range CanonicalizedMQSHeaders {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	x_mqs_headers_string := ""
	for _, v := range keys {
		x_mqs_headers_string = fmt.Sprintf("%s%s:%s\n", strings.ToLower(x_mqs_headers_string), v, CanonicalizedMQSHeaders[v])
	}
	string2sign := fmt.Sprintf("%s\n%s\n%s\n%s\n%s%s", verb, content_md5, content_type, gmt_date, x_mqs_headers_string, CanonicalizedResource)
	mac := hmac.New(sha1.New, []byte(this.AccessSecret))
	mac.Write([]byte(string2sign))
	sign := this.getBase64(mac.Sum(nil))
	return "MQS " + this.AccessKey + ":" + sign
}

// @Title 创建一个新的消息队列
// @Param queuename 队列名称
// @Param param 参数
func (this *Queue) CreateQueue(queuename string, param map[string]int) (string, error) {
	//默认参数
	_param := map[string]int{"DelaySeconds": 0, "MaximumMessageSize": 65536, "MessageRetentionPeriod": 345600, "VisibilityTimeout": 30, "PollingWaitSeconds": 0}
	for k, _ := range _param {
		for x, y := range param {
			if x == k {
				_param[k] = y
			}
		}
	}

	_xml_param := struct {
		XMLName                xml.Name `xml:"Queue"`
		Xmlns                  string   `xml:"xmlns,attr"`
		DelaySeconds           int      `xml:"DelaySeconds,omitempty"`
		MaximumMessageSize     int      `xml:"MaximumMessageSize,omitempty"`
		MessageRetentionPeriod int      `xml:"MessageRetentionPeriod,omitempty"`
		VisibilityTimeout      int      `xml:"VisibilityTimeout,omitempty"`
		PollingWaitSeconds     int      `xml:"PollingWaitSeconds,omitempty"`
	}{
		Xmlns:                  "http://mqs.aliyuncs.com/doc/v1/",
		DelaySeconds:           _param["DelaySeconds"],
		MaximumMessageSize:     _param["MaximumMessageSize"],
		MessageRetentionPeriod: _param["MessageRetentionPeriod"],
		VisibilityTimeout:      _param["VisibilityTimeout"],
		PollingWaitSeconds:     _param["PollingWaitSeconds"]}
	content_body, err := this.toXml(_xml_param)
	if err != nil {
		return "生成xml失败", err
	}
	var body bytes.Buffer
	fmt.Fprintf(&body, xml.Header)
	body.Write(content_body)

	verb := "PUT"
	content_md5 := this.getBase64([]byte(this.getMd5(body.Bytes())))
	content_type := this.ContentType
	gmt_date := this.getGMTDate()
	CanonicalizedResource := "/" + queuename
	CanonicalizedMQSHeaders := map[string]string{"x-mqs-version": this.MqsHeaders}

	sign := this.getSignature(verb, content_md5, content_type, gmt_date, CanonicalizedResource, CanonicalizedMQSHeaders)

	headers := map[string]string{
		"Host":           this.QueueOwnId + "." + this.MqsUrl,
		"Date":           gmt_date,
		"Content-Type":   content_type,
		"Content-MD5":    content_md5,
		"Authorization":  sign,
		"Content-Length": strconv.Itoa(len(body.Bytes())),
	}
	for k, v := range CanonicalizedMQSHeaders {
		headers[k] = v
	}

	request_uri := "http://" + this.QueueOwnId + "." + this.MqsUrl + CanonicalizedResource

	return this.httpClient(verb, request_uri, headers, body.String())
}

// @Title 修改消息队列属性
// @Param queuename 队列名称
// @Param param 参数
func (this *Queue) SetQueueAttributes(queuename string, param map[string]int) (string, error) {
	//默认参数
	_param := map[string]int{"DelaySeconds": 0, "MaximumMessageSize": 65536, "MessageRetentionPeriod": 345600, "VisibilityTimeout": 30, "PollingWaitSeconds": 0}
	for k, _ := range _param {
		for x, y := range param {
			if x == k {
				_param[k] = y
			}
		}
	}

	_xml_param := struct {
		XMLName                xml.Name `xml:"Queue"`
		Xmlns                  string   `xml:"xmlns,attr"`
		DelaySeconds           int      `xml:"DelaySeconds,omitempty"`
		MaximumMessageSize     int      `xml:"MaximumMessageSize,omitempty"`
		MessageRetentionPeriod int      `xml:"MessageRetentionPeriod,omitempty"`
		VisibilityTimeout      int      `xml:"VisibilityTimeout,omitempty"`
		PollingWaitSeconds     int      `xml:"PollingWaitSeconds,omitempty"`
	}{
		Xmlns:                  "http://mqs.aliyuncs.com/doc/v1/",
		DelaySeconds:           _param["DelaySeconds"],
		MaximumMessageSize:     _param["MaximumMessageSize"],
		MessageRetentionPeriod: _param["MessageRetentionPeriod"],
		VisibilityTimeout:      _param["VisibilityTimeout"],
		PollingWaitSeconds:     _param["PollingWaitSeconds"]}
	content_body, err := this.toXml(_xml_param)
	if err != nil {
		return "生成xml失败", err
	}
	var body bytes.Buffer
	fmt.Fprintf(&body, xml.Header)
	body.Write(content_body)

	verb := "PUT"
	content_md5 := this.getBase64([]byte(this.getMd5(body.Bytes())))
	content_type := this.ContentType
	gmt_date := this.getGMTDate()
	CanonicalizedResource := "/" + queuename + "?metaoverride=true"
	CanonicalizedMQSHeaders := map[string]string{"x-mqs-version": this.MqsHeaders}

	sign := this.getSignature(verb, content_md5, content_type, gmt_date, CanonicalizedResource, CanonicalizedMQSHeaders)

	headers := map[string]string{
		"Host":           this.QueueOwnId + "." + this.MqsUrl,
		"Date":           gmt_date,
		"Content-Type":   content_type,
		"Content-MD5":    content_md5,
		"Authorization":  sign,
		"Content-Length": strconv.Itoa(len(body.Bytes())),
	}
	for k, v := range CanonicalizedMQSHeaders {
		headers[k] = v
	}

	request_uri := "http://" + this.QueueOwnId + "." + this.MqsUrl + CanonicalizedResource

	return this.httpClient(verb, request_uri, headers, body.String())

}

// @Title 获取某个已创建的消息队列的属性
// @Param queuename 队列名称
func (this *Queue) GetQueueAttributes(queuename string) (string, error) {
	verb := "GET"
	content_body := ""
	content_md5 := this.getBase64([]byte(content_body))
	content_type := this.ContentType
	gmt_date := this.getGMTDate()
	CanonicalizedResource := "/" + queuename
	CanonicalizedMQSHeaders := map[string]string{"x-mqs-version": this.MqsHeaders}

	sign := this.getSignature(verb, content_md5, content_type, gmt_date, CanonicalizedResource, CanonicalizedMQSHeaders)

	headers := map[string]string{
		"Host":           this.QueueOwnId + "." + this.MqsUrl,
		"Date":           gmt_date,
		"Content-Type":   content_type,
		"Content-MD5":    content_md5,
		"Authorization":  sign,
		"Content-Length": strconv.Itoa(len(content_body)),
	}
	for k, v := range CanonicalizedMQSHeaders {
		headers[k] = v
	}

	request_uri := "http://" + this.QueueOwnId + "." + this.MqsUrl + CanonicalizedResource

	return this.httpClient(verb, request_uri, headers, content_body)
}

// @Title 用于删除一个已创建的消息队列
// @Param queuename 队列名称
func (this *Queue) DeleteQueue(queuename string) (string, error) {

	verb := "DELETE"
	content_body := ""
	content_md5 := this.getBase64([]byte(content_body))
	content_type := this.ContentType
	gmt_date := this.getGMTDate()
	CanonicalizedResource := "/" + queuename
	CanonicalizedMQSHeaders := map[string]string{"x-mqs-version": this.MqsHeaders}

	sign := this.getSignature(verb, content_md5, content_type, gmt_date, CanonicalizedResource, CanonicalizedMQSHeaders)

	headers := map[string]string{
		"Host":           this.QueueOwnId + "." + this.MqsUrl,
		"Date":           gmt_date,
		"Content-Type":   content_type,
		"Content-MD5":    content_md5,
		"Authorization":  sign,
		"Content-Length": strconv.Itoa(len(content_body)),
	}
	for k, v := range CanonicalizedMQSHeaders {
		headers[k] = v
	}

	request_uri := "http://" + this.QueueOwnId + "." + this.MqsUrl + CanonicalizedResource

	return this.httpClient(verb, request_uri, headers, content_body)
}

// @Title 用于列出 QueueOwnerId 下的消息队列列表,可分页获取数据
// @Param prefix	按照该前缀开头的 queueName 进行查找
// @Param marker	请求下一个分页的开始位置,一般从上 次分页结果返回的 NextMarker 获取
// @Param number	单次请求结果的最大返回个数,可以取 1-1000 范围内的整数值,默认值为 1000
func (this *Queue) ListQueue(prefix, marker, number string) (string, error) {
	verb := "GET"
	content_body := ""
	content_md5 := this.getBase64([]byte(content_body))
	content_type := this.ContentType
	gmt_date := this.getGMTDate()
	CanonicalizedResource := "/"
	CanonicalizedMQSHeaders := map[string]string{"x-mqs-version": this.MqsHeaders}
	if strings.TrimSpace(prefix) != "" {
		CanonicalizedMQSHeaders["x-mqs-prefix"] = prefix
	}
	if strings.TrimSpace(marker) != "" {
		CanonicalizedMQSHeaders["x-mqs-marker'"] = marker
	}
	if strings.TrimSpace(number) != "" {
		CanonicalizedMQSHeaders["x-mqs-ret-number"] = number
	}

	sign := this.getSignature(verb, content_md5, content_type, gmt_date, CanonicalizedResource, CanonicalizedMQSHeaders)

	headers := map[string]string{
		"Host":           this.QueueOwnId + "." + this.MqsUrl,
		"Date":           gmt_date,
		"Content-Type":   content_type,
		"Content-MD5":    content_md5,
		"Authorization":  sign,
		"Content-Length": strconv.Itoa(len(content_body)),
	}
	for k, v := range CanonicalizedMQSHeaders {
		headers[k] = v
	}

	request_uri := "http://" + this.QueueOwnId + "." + this.MqsUrl + CanonicalizedResource

	return this.httpClient(verb, request_uri, headers, content_body)
}

// @Title 发送消息到指定的消息队列
// @Param queuename 	队列名称
// @Param messagebody 	消息正文
// @Param param 		参数
//        -- delayseconds 	指定 的秒数延后可被消费,单 位为秒，0-345600 秒(4 天)范围内 某个整数值
// 		  -- priority 		指定消息的优先级 权值。优先级越高的消 息,越容易更早被消费，取值范围 1~16(其中 1 为 最高优先级),默认优先级 为8
func (this *Message) SendMessage(queuename, messagebody string, param map[string]int) (string, error) {
	//默认参数
	_param := map[string]int{"DelaySeconds": 0, "Priority": 8}
	for k, _ := range _param {
		for x, y := range param {
			if x == k {
				_param[k] = y
			}
		}
	}

	_xml_param := struct {
		XMLName      xml.Name `xml:"Message"`
		Xmlns        string   `xml:"xmlns,attr"`
		MessageBody  string   `xml:"MessageBody,omitempty"`
		DelaySeconds int      `xml:"DelaySeconds,omitempty"`
		Priority     int      `xml:"Priority,omitempty"`
	}{
		Xmlns:        "http://mqs.aliyuncs.com/doc/v1/",
		MessageBody:  messagebody,
		DelaySeconds: _param["DelaySeconds"],
		Priority:     _param["Priority"]}
	content_body, err := this.toXml(_xml_param)
	if err != nil {
		return "生成xml失败", err
	}

	verb := "POST"
	content_md5 := this.getBase64([]byte(this.getMd5(content_body)))
	content_type := this.ContentType
	gmt_date := this.getGMTDate()
	CanonicalizedResource := "/" + queuename + "/messages"
	CanonicalizedMQSHeaders := map[string]string{"x-mqs-version": this.MqsHeaders}

	sign := this.getSignature(verb, content_md5, content_type, gmt_date, CanonicalizedResource, CanonicalizedMQSHeaders)

	headers := map[string]string{
		"Host":           this.QueueOwnId + "." + this.MqsUrl,
		"Date":           gmt_date,
		"Content-Type":   content_type,
		"Content-MD5":    content_md5,
		"Authorization":  sign,
		"Content-Length": strconv.Itoa(len(content_body)),
	}
	for k, v := range CanonicalizedMQSHeaders {
		headers[k] = v
	}

	request_uri := "http://" + this.QueueOwnId + "." + this.MqsUrl + CanonicalizedResource

	return this.httpClient(verb, request_uri, headers, string(content_body))

}

// @Title 用于消费者消费消息队列的消息
// @Param queuename		队列名称
// @Param waitseconds 	本次 ReceiveMessage 请求最长的 Polling 等待时间1,单位为秒
func (this *Message) ReceiveMessage(queuename string, waitseconds int) (string, error) {
	verb := "GET"
	content_body := ""
	content_md5 := ""
	content_type := this.ContentType
	gmt_date := this.getGMTDate()
	CanonicalizedResource := fmt.Sprintf("/%s/messages?waitseconds=%d", queuename, waitseconds)
	CanonicalizedMQSHeaders := map[string]string{"x-mqs-version": this.MqsHeaders}

	sign := this.getSignature(verb, content_md5, content_type, gmt_date, CanonicalizedResource, CanonicalizedMQSHeaders)

	headers := map[string]string{
		"Host":           this.QueueOwnId + "." + this.MqsUrl,
		"Date":           gmt_date,
		"Content-Type":   content_type,
		"Content-MD5":    content_md5,
		"Authorization":  sign,
		"Content-Length": strconv.Itoa(len(content_body)),
	}
	for k, v := range CanonicalizedMQSHeaders {
		headers[k] = v
	}

	request_uri := "http://" + this.QueueOwnId + "." + this.MqsUrl + CanonicalizedResource

	return this.httpClient(verb, request_uri, headers, string(content_body))
}

// @Title 用于删除已经被消费过的消息
// @Param queuename		队列名称
// @Param ReceiptHandle 上次消费后返回的消息
func (this *Message) DeleteMessage(queuename, receipthandle string) (string, error) {
	verb := "DELETE"
	content_body := ""
	content_md5 := ""
	content_type := this.ContentType
	gmt_date := this.getGMTDate()
	CanonicalizedResource := fmt.Sprintf("/%s/messages?ReceiptHandle=%d", queuename, receipthandle)
	CanonicalizedMQSHeaders := map[string]string{"x-mqs-version": this.MqsHeaders}

	sign := this.getSignature(verb, content_md5, content_type, gmt_date, CanonicalizedResource, CanonicalizedMQSHeaders)

	headers := map[string]string{
		"Host":           this.QueueOwnId + "." + this.MqsUrl,
		"Date":           gmt_date,
		"Content-Type":   content_type,
		"Content-MD5":    content_md5,
		"Authorization":  sign,
		"Content-Length": strconv.Itoa(len(content_body)),
	}
	for k, v := range CanonicalizedMQSHeaders {
		headers[k] = v
	}

	request_uri := "http://" + this.QueueOwnId + "." + this.MqsUrl + CanonicalizedResource

	return this.httpClient(verb, request_uri, headers, string(content_body))
}

// @Title 用于消费者查看消息 PeekMessage 与 ReceiveMessage 不同, PeekMessage 并不会改变消息的状态,
//        即被 PeekMessage 获取消息后 消息仍然处于 Active 状态,仍然可被查看或消费;而后者操作成功 后消息进入 Inactive,
//        在 VisibilityTimeout 的时间内不可被查看或消费
// @Param queuename		队列名称
func (this *Message) PeekMessage(queuename string) (string, error) {
	verb := "GET"
	content_body := ""
	content_md5 := ""
	content_type := this.ContentType
	gmt_date := this.getGMTDate()
	CanonicalizedResource := fmt.Sprintf("/%s/messages?peekonly=true", queuename)
	CanonicalizedMQSHeaders := map[string]string{"x-mqs-version": this.MqsHeaders}

	sign := this.getSignature(verb, content_md5, content_type, gmt_date, CanonicalizedResource, CanonicalizedMQSHeaders)

	headers := map[string]string{
		"Host":           this.QueueOwnId + "." + this.MqsUrl,
		"Date":           gmt_date,
		"Content-Type":   content_type,
		"Content-MD5":    content_md5,
		"Authorization":  sign,
		"Content-Length": strconv.Itoa(len(content_body)),
	}
	for k, v := range CanonicalizedMQSHeaders {
		headers[k] = v
	}

	request_uri := "http://" + this.QueueOwnId + "." + this.MqsUrl + CanonicalizedResource

	return this.httpClient(verb, request_uri, headers, string(content_body))
}

// @Title 用于修改被消费过并且还处于的 Inactive 的消息到下次可被消费的时间,成功修改消息的 VisibilityTimeout 后,返回新的 ReceiptHandle
// @Param queuename			队列名称
// @Param receipthandle		上次消费后返回的消息 ReceiptHandle,详 见本文 ReceiveMessage 接口
// @Param visibilitytimeout	从现在到下次可被用来消费的时间间隔,单位为秒
func (this *Message) ChangeMessageVisibility(queuename, receipthandle string, visibilitytimeout int) (string, error) {
	verb := "PUT"
	content_body := ""
	content_md5 := ""
	content_type := this.ContentType
	gmt_date := this.getGMTDate()
	CanonicalizedResource := fmt.Sprintf("/%s/messages?ReceiptHandle=%s&VisibilityTimeout=%d", queuename, receipthandle, visibilitytimeout)
	CanonicalizedMQSHeaders := map[string]string{"x-mqs-version": this.MqsHeaders}

	sign := this.getSignature(verb, content_md5, content_type, gmt_date, CanonicalizedResource, CanonicalizedMQSHeaders)

	headers := map[string]string{
		"Host":           this.QueueOwnId + "." + this.MqsUrl,
		"Date":           gmt_date,
		"Content-Type":   content_type,
		"Content-MD5":    content_md5,
		"Authorization":  sign,
		"Content-Length": strconv.Itoa(len(content_body)),
	}
	for k, v := range CanonicalizedMQSHeaders {
		headers[k] = v
	}

	request_uri := "http://" + this.QueueOwnId + "." + this.MqsUrl + CanonicalizedResource

	return this.httpClient(verb, request_uri, headers, string(content_body))
}
