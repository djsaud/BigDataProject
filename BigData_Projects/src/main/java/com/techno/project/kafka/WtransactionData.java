package com.techno.project.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "id",
        "time_created",
        "time_processed",
        "flags",
        "account_number",
        "transaction_id",
        "counterparty",
        "payee_email",
        "amount",
        "status",
        "type",
        "reason",
        "time_received_payee",
        "time_created_payer",
        "memo",
        "payment_id",
        "ach_id",
        "sync_group",
        "address_id",
        "payee_email_upper",
        "parent_id",
        "shared_id",
        "cctrans_id",
        "counterparty_alias",
        "counterparty_alias_type",
        "counterparty_alias_upper",
        "message",
        "time_user",
        "message_id",
        "subtype",
        "flags2",
        "time_inactive",
        "target_alias_id",
        "counterparty_last_login_ip",
        "balance_at_time_created",
        "accept_deny_method",
        "currency_code",
        "usd_amount",
        "base_id",
        "flags3",
        "time_updated",
        "transition",
        "flags4",
        "time_row_updated",
        "flags5",
        "flags6",
        "flags7"
})
public class WtransactionData {
    // name conventions were changed as per the Sample json earlier the used the variables as camelcase
    @JsonProperty("id")
    private Integer id;
    @JsonProperty("time_created")
    private String time_created;
    @JsonProperty("time_processed")
    private String time_processed;
    @JsonProperty("flags")
    private String flags;
    @JsonProperty("account_number")
    private Integer account_number;
    @JsonProperty("transaction_id")
    private String transaction_id;
    @JsonProperty("counterparty")
    private String counterparty;
    @JsonProperty("payee_email")
    private String payee_email;
    @JsonProperty("amount")
    private String amount;
    @JsonProperty("status")
    private String status;
    @JsonProperty("type")
    private String type;
    @JsonProperty("reason")
    private String reason;
    @JsonProperty("time_received_payee")
    private String time_received_payee;
    @JsonProperty("time_created_payer")
    private String time_created_payer;
    @JsonProperty("memo")
    private String memo;
    @JsonProperty("payment_id")
    private String payment_id;
    @JsonProperty("ach_id")
    private String ach_id;
    @JsonProperty("sync_group")
    private String sync_group;
    @JsonProperty("address_id")
    private String address_id;
    @JsonProperty("payee_email_upper")
    private String payee_email_upper;
    @JsonProperty("parent_id")
    private String parent_id;
    @JsonProperty("shared_id")
    private String shared_id;
    @JsonProperty("cctrans_id")
    private String cctrans_id;
    @JsonProperty("counterparty_alias")
    private String counterparty_alias;
    @JsonProperty("counterparty_alias_type")
    private String counterparty_alias_type;
    @JsonProperty("counterparty_alias_upper")
    private String counterparty_alias_upper;
    @JsonProperty("message")
    private String message;
    @JsonProperty("time_user")
    private String time_user;
    @JsonProperty("message_id")
    private String message_id;
    @JsonProperty("subtype")
    private String subtype;
    @JsonProperty("flags2")
    private String flags2;
    @JsonProperty("time_inactive")
    private String time_inactive;
    @JsonProperty("target_alias_id")
    private String target_alias_id;
    @JsonProperty("counterparty_last_login_ip")
    private String counterparty_last_login_ip;
    @JsonProperty("balance_at_time_created")
    private String balance_at_time_created;
    @JsonProperty("accept_deny_method")
    private String accept_deny_method;
    @JsonProperty("currency_code")
    private String currency_code;
    @JsonProperty("usd_amount")
    private String usd_amount;
    @JsonProperty("base_id")
    private String base_id;
    @JsonProperty("flags3")
    private String flags3;
    @JsonProperty("time_updated")
    private String time_updated;
    @JsonProperty("transition")
    private String transition;
    @JsonProperty("flags4")
    private String flags4;
    @JsonProperty("time_row_updated")
    private String time_row_updated;
    @JsonProperty("flags5")
    private String flags5;
    @JsonProperty("flags6")
    private String flags6;
    @JsonProperty("flags7")
    private String flags7;


    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getTime_created() {
        return time_created;
    }

    public void setTime_created(String time_created) {
        this.time_created = time_created;
    }

    public String getTime_processed() {
        return time_processed;
    }

    public void setTime_processed(String time_processed) {
        this.time_processed = time_processed;
    }

    public String getFlags() {
        return flags;
    }

    public void setFlags(String flags) {
        this.flags = flags;
    }

    public Integer getAccount_number() {
        return account_number;
    }

    public void setAccount_number(Integer account_number) {
        this.account_number = account_number;
    }

    public String getTransaction_id() {
        return transaction_id;
    }

    public void setTransaction_id(String transaction_id) {
        this.transaction_id = transaction_id;
    }

    public String getCounterparty() {
        return counterparty;
    }

    public void setCounterparty(String counterparty) {
        this.counterparty = counterparty;
    }

    public String getPayee_email() {
        return payee_email;
    }

    public void setPayee_email(String payee_email) {
        this.payee_email = payee_email;
    }

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public String getTime_received_payee() {
        return time_received_payee;
    }

    public void setTime_received_payee(String time_received_payee) {
        this.time_received_payee = time_received_payee;
    }

    public String getTime_created_payer() {
        return time_created_payer;
    }

    public void setTime_created_payer(String time_created_payer) {
        this.time_created_payer = time_created_payer;
    }

    public String getMemo() {
        return memo;
    }

    public void setMemo(String memo) {
        this.memo = memo;
    }

    public String getPayment_id() {
        return payment_id;
    }

    public void setPayment_id(String payment_id) {
        this.payment_id = payment_id;
    }

    public String getAch_id() {
        return ach_id;
    }

    public void setAch_id(String ach_id) {
        this.ach_id = ach_id;
    }

    public String getSync_group() {
        return sync_group;
    }

    public void setSync_group(String sync_group) {
        this.sync_group = sync_group;
    }

    public String getAddress_id() {
        return address_id;
    }

    public void setAddress_id(String address_id) {
        this.address_id = address_id;
    }

    public String getPayee_email_upper() {
        return payee_email_upper;
    }

    public void setPayee_email_upper(String payee_email_upper) {
        this.payee_email_upper = payee_email_upper;
    }

    public String getParent_id() {
        return parent_id;
    }

    public void setParent_id(String parent_id) {
        this.parent_id = parent_id;
    }

    public String getShared_id() {
        return shared_id;
    }

    public void setShared_id(String shared_id) {
        this.shared_id = shared_id;
    }

    public String getCctrans_id() {
        return cctrans_id;
    }

    public void setCctrans_id(String cctrans_id) {
        this.cctrans_id = cctrans_id;
    }

    public String getCounterparty_alias() {
        return counterparty_alias;
    }

    public void setCounterparty_alias(String counterparty_alias) {
        this.counterparty_alias = counterparty_alias;
    }

    public String getCounterparty_alias_type() {
        return counterparty_alias_type;
    }

    public void setCounterparty_alias_type(String counterparty_alias_type) {
        this.counterparty_alias_type = counterparty_alias_type;
    }

    public String getCounterparty_alias_upper() {
        return counterparty_alias_upper;
    }

    public void setCounterparty_alias_upper(String counterparty_alias_upper) {
        this.counterparty_alias_upper = counterparty_alias_upper;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getTime_user() {
        return time_user;
    }

    public void setTime_user(String time_user) {
        this.time_user = time_user;
    }

    public String getMessage_id() {
        return message_id;
    }

    public void setMessage_id(String message_id) {
        this.message_id = message_id;
    }

    public String getSubtype() {
        return subtype;
    }

    public void setSubtype(String subtype) {
        this.subtype = subtype;
    }

    public String getFlags2() {
        return flags2;
    }

    public void setFlags2(String flags2) {
        this.flags2 = flags2;
    }

    public String getTime_inactive() {
        return time_inactive;
    }

    public void setTime_inactive(String time_inactive) {
        this.time_inactive = time_inactive;
    }

    public String getTarget_alias_id() {
        return target_alias_id;
    }

    public void setTarget_alias_id(String target_alias_id) {
        this.target_alias_id = target_alias_id;
    }

    public String getCounterparty_last_login_ip() {
        return counterparty_last_login_ip;
    }

    public void setCounterparty_last_login_ip(String counterparty_last_login_ip) {
        this.counterparty_last_login_ip = counterparty_last_login_ip;
    }

    public String getBalance_at_time_created() {
        return balance_at_time_created;
    }

    public void setBalance_at_time_created(String balance_at_time_created) {
        this.balance_at_time_created = balance_at_time_created;
    }

    public String getAccept_deny_method() {
        return accept_deny_method;
    }

    public void setAccept_deny_method(String accept_deny_method) {
        this.accept_deny_method = accept_deny_method;
    }

    public String getCurrency_code() {
        return currency_code;
    }

    public void setCurrency_code(String currency_code) {
        this.currency_code = currency_code;
    }

    public String getUsd_amount() {
        return usd_amount;
    }

    public void setUsd_amount(String usd_amount) {
        this.usd_amount = usd_amount;
    }

    public String getBase_id() {
        return base_id;
    }

    public void setBase_id(String base_id) {
        this.base_id = base_id;
    }

    public String getFlags3() {
        return flags3;
    }

    public void setFlags3(String flags3) {
        this.flags3 = flags3;
    }

    public String getTime_updated() {
        return time_updated;
    }

    public void setTime_updated(String time_updated) {
        this.time_updated = time_updated;
    }

    public String getTransition() {
        return transition;
    }

    public void setTransition(String transition) {
        this.transition = transition;
    }

    public String getFlags4() {
        return flags4;
    }

    public void setFlags4(String flags4) {
        this.flags4 = flags4;
    }

    public String getTime_row_updated() {
        return time_row_updated;
    }

    public void setTime_row_updated(String time_row_updated) {
        this.time_row_updated = time_row_updated;
    }

    public String getFlags5() {
        return flags5;
    }

    public void setFlags5(String flags5) {
        this.flags5 = flags5;
    }

    public String getFlags6() {
        return flags6;
    }

    public void setFlags6(String flags6) {
        this.flags6 = flags6;
    }

    public String getFlags7() {
        return flags7;
    }

    public void setFlags7(String flags7) {
        this.flags7 = flags7;
    }


}