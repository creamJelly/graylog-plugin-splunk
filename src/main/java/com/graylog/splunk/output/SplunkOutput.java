/**
 * This file is part of Graylog.
 *
 * Graylog is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.graylog.splunk.output;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.graylog.splunk.output.senders.*;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.DropdownField;
import org.graylog2.plugin.configuration.fields.NumberField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.inputs.annotations.ConfigClass;
import org.graylog2.plugin.inputs.annotations.FactoryClass;
import org.graylog2.plugin.outputs.MessageOutput;
import org.graylog2.plugin.outputs.MessageOutputConfigurationException;
import org.graylog2.plugin.streams.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class SplunkOutput implements MessageOutput {

    private static final Logger LOG = LoggerFactory.getLogger(SplunkOutput.class);

    private static final String CK_SPLUNK_HOST = "splunk_host";
    private static final String CK_SPLUNK_PORT = "splunk_port";
    private static final String CK_SPLUNK_PROTOCOL = "splunk_protocol";
    private static final String CK_SPLUNK_PARAMS = "splunk_params";

    private boolean running = true;

    private final Sender sender;

    @Inject
    public SplunkOutput(@Assisted Configuration configuration) throws MessageOutputConfigurationException {
        // Check configuration.
        if (!checkConfiguration(configuration)) {
            throw new MessageOutputConfigurationException("Missing configuration.");
        }

        // Set up sender.
//        sender = new TCPSender(
//                configuration.getString(CK_SPLUNK_HOST),
//                configuration.getInt(CK_SPLUNK_PORT)
//        );
//        sender = new UDPSender(
//                configuration.getString(CK_SPLUNK_HOST),
//                configuration.getInt(CK_SPLUNK_PORT),
//                configuration.getString(CK_SPLUNK_PARAMS)
//        );
//        sender = new UDPSender_2(
//                configuration.getString(CK_SPLUNK_HOST),
//                configuration.getInt(CK_SPLUNK_PORT),
//                configuration.getString(CK_SPLUNK_PARAMS)
//        );
        sender = new UDPSender_3(
                configuration.getString(CK_SPLUNK_HOST),
                configuration.getInt(CK_SPLUNK_PORT),
                configuration.getString(CK_SPLUNK_PARAMS)
        );
        running = true;
    }

    @Override
    public void stop() {
        sender.stop();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public void write(Message message) throws Exception {
        if (message == null || message.getFields() == null || message.getFields().isEmpty()) {
            return;
        }

        if(!sender.isInitialized()) {
            sender.initialize();
        }

        System.out.println("before Send");
        sender.send(message);
        System.out.println("after send");
    }

    @Override
    public void write(List<Message> list) throws Exception {
        if (list == null) {
            return;
        }

        for(Message m : list) {
            write(m);
        }
    }

    public boolean checkConfiguration(Configuration c) {
        return c.stringIsSet(CK_SPLUNK_HOST)
                && c.intIsSet(CK_SPLUNK_PORT)
                && c.stringIsSet(CK_SPLUNK_PROTOCOL)
                && ("UDP".equals(c.getString(CK_SPLUNK_PROTOCOL)) || "TCP".equals(c.getString(CK_SPLUNK_PROTOCOL)));
    }

    @FactoryClass
    public interface Factory extends MessageOutput.Factory<SplunkOutput> {
        @Override
        SplunkOutput create(Stream stream, Configuration configuration);

        @Override
        Config getConfig();

        @Override
        Descriptor getDescriptor();
    }

    @ConfigClass
    public static class Config extends MessageOutput.Config {
        @Override
        public ConfigurationRequest getRequestedConfiguration() {
            final ConfigurationRequest configurationRequest = new ConfigurationRequest();

            configurationRequest.addField(new TextField(
                            CK_SPLUNK_HOST, "Splunk Host", "",
                            "目标域名或IP",
                            ConfigurationField.Optional.NOT_OPTIONAL)
            );

            configurationRequest.addField(new NumberField(
                            CK_SPLUNK_PORT, "Splunk Port", 12999,
                            "端口号",
                            ConfigurationField.Optional.OPTIONAL)
            );

            final Map<String, String> protocols = ImmutableMap.of("UDP", "UDP");
            configurationRequest.addField(new DropdownField(
                            CK_SPLUNK_PROTOCOL, "Splunk Protocol", "UDP", protocols,
                            "协议类型",
                            ConfigurationField.Optional.OPTIONAL)
            );

            configurationRequest.addField(new TextField(
                    CK_SPLUNK_PARAMS, "params", "",
                    "参数列表",
                    ConfigurationField.Optional.NOT_OPTIONAL)
            );

            return configurationRequest;
        }
    }

    public static class Descriptor extends MessageOutput.Descriptor {
        public Descriptor() {
            super("Splunk Output", false, "", "Writes messages to your Splunk installation via UDP or TCP.");
        }
    }

}
