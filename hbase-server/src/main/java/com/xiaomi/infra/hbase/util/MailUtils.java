/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xiaomi.infra.hbase.util;

import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.mail.smtp.SMTPTransport;

public class MailUtils {
	private static final Logger LOG = LoggerFactory.getLogger(MailUtils.class.getName());
	private static final Properties PROPERTIES = System.getProperties();

	static {
		PROPERTIES.setProperty("mail.smtp.host", "mail.b2c.srv");
		PROPERTIES.setProperty("mail.transport.protocol", "smtp");
	}

	public static void sendMail(Configuration conf, String to, String subject, String body) {
		Session session = Session.getDefaultInstance(PROPERTIES);

		MimeMessage message = new MimeMessage(session);
		try {
			SMTPTransport transport = (SMTPTransport) session.getTransport();
			transport.setLocalHost("127.0.0.1");
			message.addRecipient(Message.RecipientType.TO, new InternetAddress(to));
			message.setSubject(subject);
			message.setText(body);
			message.setFrom(new InternetAddress(conf.get("hbase.xiaomi.mail.from", "")));
			SMTPTransport.send(message);
		} catch (Throwable e) {
			LOG.error("Failed to mail to {} subject {}, body {}", new Object[] { to, subject, body, e });
		}
	}
}
