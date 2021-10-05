package com.techy.betterreadsdataloader.connection;

import java.io.File;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "datastax.astra")
public class DataStaxAstraProperties {
    private File SecureConnectBundle;

    public File getSecureConnectBundle() {
        return SecureConnectBundle;
    }

    public void setSecureConnectBundle(File secureConnectBundle) {
        SecureConnectBundle = secureConnectBundle;
    }

    
}
