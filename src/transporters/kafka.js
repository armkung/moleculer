/*
 * moleculer
 * Copyright (c) 2018 MoleculerJS (https://github.com/moleculerjs/moleculer)
 * MIT Licensed
 */

"use strict";

const { defaultsDeep } = require("lodash");
const chalk = require("chalk");
const Promise = require("bluebird");
const Transporter = require("./base");

/**
 * Lightweight transporter for Kafka
 *
 * For test:
 *   1. clone https://github.com/wurstmeister/kafka-docker.git repo
 *   2. follow instructions on https://github.com/wurstmeister/kafka-docker#pre-requisites
 * 	 3. start containers with Docker Compose
 *
 * 			docker-compose -f docker-compose-single-broker.yml up -d
 *
 * @class KafkaTransporter
 * @extends {Transporter}
 */
class KafkaTransporter extends Transporter {
	/**
	 * Creates an instance of KafkaTransporter.
	 *
	 * @param {any} opts
	 *
	 * @memberof KafkaTransporter
	 */
	constructor(opts) {
		if (typeof opts === "string") {
			opts = {
				clientId: "app",
				brokers: [opts.replace("kafka://", "")]
			};
		} else if (opts == null) {
			opts = {};
		}

		super(opts);

		const Kafka = require("kafkajs").Kafka;

		this.kafka = new Kafka(opts);

		this.producer = null;
		this.consumer = null;
	}

	/**
	 * Connect to the server
	 *
	 * @memberof KafkaTransporter
	 */
	async connect() {
		try {
			this.producer = this.kafka.producer();

			await this.producer.connect();

			this.logger.info("Kafka client is connected.");

			await this.onConnected();
		} catch (err) {
			this.logger.error("Kafka Client error", err.message);
			this.logger.debug(err);

			if (!this.connected) return Promise.reject(err);
		}
	}

	/**
	 * Disconnect from the server
	 *
	 * @memberof KafkaTransporter
	 */
	async disconnect() {
		this.producer = null;
		this.consumer = null;
	}

	/**
	 * Subscribe to all topics
	 *
	 * @param {Array<Object>} topics
	 *
	 * @memberof BaseTransporter
	 */
	async makeSubscriptions(topics) {
		try {
			this.consumer = this.kafka.consumer({ groupId: this.nodeID });

			await this.consumer.connect();
			await Promise.all(
				topics.map(({ cmd, nodeID }) => {
					return this.consumer.subscribe({
						topic: this.getTopicName(cmd, nodeID),
						fromBeginning: false
					});
				})
			);

			await this.consumer.run({
				eachMessage: async ({ topic, message }) => {
					const cmd = topic.split(".")[1];
					this.incomingMessage(cmd, message.value);
				}
			});
		} catch (err) {
			this.logger.error("Kafka Consumer error", err.message);
			this.logger.debug(err);

			if (!this.connected) return Promise.reject(err);
		}
	}

	/**
	 * Subscribe to a command
	 *
	 * @param {String} cmd
	 * @param {String} nodeID
	 *
	 * @memberof KafkaTransporter
	 */
	/*
	subscribe(cmd, nodeID) {
		const topic = this.getTopicName(cmd, nodeID);
		this.topics.push(topic);

		return new Promise((resolve, reject) => {
			this.producer.createTopics([topic], true, (err, data) => {
				if (err) {
					this.logger.error("Unable to create topics!", topic, err);
					return reject(err);
				}

				this.consumer.addTopics([{ topic, offset: -1 }], (err, added) => {
					if (err) {
						this.logger.error("Unable to add topic!", topic, err);
						return reject(err);
					}

					resolve();
				}, false);
			});
		});
	}*/

	/**
	 * Publish a packet
	 *
	 * @param {Packet} packet
	 *
	 * @memberof KafkaTransporter
	 */
	async publish(packet) {
		/* istanbul ignore next */
		if (!this.producer) return Promise.resolve();

		try {
			const data = this.serialize(packet);
			this.incStatSent(data.length);

			await this.producer.send({
				topic: this.getTopicName(packet.type, packet.target),
				messages: [data]
			});
		} catch (err) {
			this.logger.error("Kafka Publish error", err.message);
			this.logger.debug(err);

			if (!this.connected) return Promise.reject(err);
		}
	}
}

module.exports = KafkaTransporter;
