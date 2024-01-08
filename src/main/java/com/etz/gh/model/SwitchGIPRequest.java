package com.etz.gh.model;

public class SwitchGIPRequest {
    private String network;
    private String bankCode;
    private String excludedBank;
    private String switchToGIP;

    public String getNetwork() {
        return network;
    }

    public void setNetwork(String network) {
        this.network = network;
    }

    public String getBankCode() {
        return bankCode;
    }

    public void setBankCode(String bankCode) {
        this.bankCode = bankCode;
    }

    public String getExcludedBank() {
        return excludedBank;
    }

    public void setExcludedBank(String excludedBank) {
        this.excludedBank = excludedBank;
    }

    public String getSwitchToGIP() {
        return switchToGIP;
    }

    public void setSwitchToGIP(String switchToGIP) {
        this.switchToGIP = switchToGIP;
    }
}
