package entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Date;

public class SmsLogs {
    @JsonIgnore
    private String id; // id

    @JsonFormat(pattern = "yyyy-MM-dd")
    private Date createDate; // 创建时间

    @JsonFormat(pattern = "yyyy-MM-dd")
    private Date sendDate; // 发送时间

    private String longCode; // 发送的长号码
    private String mobile; // 手机号
    private String corpName;  // 发送公司名称
    private String smsContent; // 短信内容
    private Integer start; // 短信发送状态,0成功，1失败
    private Integer operatorId; // 运营商编号 1移动 2联通 3电信
    private String province; // 省份
    private String ipAddr; // 服务器ip地址
    private Integer replyTotal; // 短信状态报告返回时长（秒）
    private Integer fee; // 费用

    public SmsLogs() {
    }

    public SmsLogs(String id, Date createDate, Date sendDate, String longCode, String mobile, String corpName, String smsContent, Integer start, Integer operatorId, String province, String ipAddr, Integer replyTotal, Integer fee) {
        this.id = id;
        this.createDate = createDate;
        this.sendDate = sendDate;
        this.longCode = longCode;
        this.mobile = mobile;
        this.corpName = corpName;
        this.smsContent = smsContent;
        this.start = start;
        this.operatorId = operatorId;
        this.province = province;
        this.ipAddr = ipAddr;
        this.replyTotal = replyTotal;
        this.fee = fee;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Date getCreateDate() {
        return createDate;
    }

    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    public Date getSendDate() {
        return sendDate;
    }

    public void setSendDate(Date sendDate) {
        this.sendDate = sendDate;
    }

    public String getLongCode() {
        return longCode;
    }

    public void setLongCode(String longCode) {
        this.longCode = longCode;
    }

    public String getMobile() {
        return mobile;
    }

    public void setMobile(String mobile) {
        this.mobile = mobile;
    }

    public String getCorpName() {
        return corpName;
    }

    public void setCorpName(String corpName) {
        this.corpName = corpName;
    }

    public String getSmsContent() {
        return smsContent;
    }

    public void setSmsContent(String smsContent) {
        this.smsContent = smsContent;
    }

    public Integer getStart() {
        return start;
    }

    public void setStart(Integer start) {
        this.start = start;
    }

    public Integer getOperatorId() {
        return operatorId;
    }

    public void setOperatorId(Integer operatorId) {
        this.operatorId = operatorId;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getIpAddr() {
        return ipAddr;
    }

    public void setIpAddr(String ipAddr) {
        this.ipAddr = ipAddr;
    }

    public Integer getReplyTotal() {
        return replyTotal;
    }

    public void setReplyTotal(Integer replyTotal) {
        this.replyTotal = replyTotal;
    }

    public Integer getFee() {
        return fee;
    }

    public void setFee(Integer fee) {
        this.fee = fee;
    }
}
