const { createClient } = require('redis');
const { v4: uuidv4 } = require('uuid');
const { z } = require('zod');

const pub = createClient();
const sub = createClient();

let connected = false;
let messageId = 1;

const connectRedis = async () => {
	if (connected) return;
	await Promise.all([pub.connect(), sub.connect()]);
	connected = true;
	console.log('[Redis] Pub/Sub connected');
};

/**
 * Sends a query to another service and waits for a response.
 * @param {string} service - Target service name.
 * @param {string} event - Event name under that service.
 * @param {object} data - Payload to send.
 * @param {object} zodType - Optional Zod schema to validate response (default: no validation).
 * @param {number} timeoutMs - Optional timeout in milliseconds (default 1000ms).
 * @returns {Promise<any>} - Resolves with response data from listener.
 */
const query = async (service, event, data,
	zodType = z.unknown(),
	timeoutMs = 1000) => {
	await connectRedis();

	const requestId = uuidv4();
	const responseChannel = `response:${service}:${requestId}`;
	const queryChannel = `query:${service}:${event}`;

	return new Promise(async (resolve, reject) => {
		let settled = false;

		const timeout = setTimeout(() => {
			if (!settled) {
				settled = true;
				sub.unsubscribe(responseChannel).catch(() => {});
				reject(new Error('Query timed out'));
			}
		}, timeoutMs);

		const onMessage = async (msg) => {
			if (settled) return;
			settled = true;
			clearTimeout(timeout);
			await sub.unsubscribe(responseChannel);

			try {
				const parsed = JSON.parse(msg);
				resolve(zodType.parse(parsed.data));
			} catch (err) {
				reject(new Error('Invalid JSON response'));
			}
		};

		await sub.subscribe(responseChannel, onMessage);

		await pub.publish(
			queryChannel,
			JSON.stringify({
				data,
				requestId,
				responseChannel,
				date: new Date().toISOString(),
				id: messageId++
			})
		);
	});
};

/**
 * Listens for queries on a specific service/event and responds with return value of handler.
 * @param {string} service - Service name.
 * @param {string} event - Event name.
 * @param {(params: { data: any, id: number, date: Date, requestId: string, responseChannel: string }) => any | Promise<any>} handler
 */
const queryListen = (service, event, handler) => {
	const channel = `query:${service}:${event}`;

	sub.subscribe(channel, async (msg) => {
		try {
			const parsed = JSON.parse(msg);

			const response = await handler({
				data: parsed.data,
				id: parsed.id,
				date: new Date(parsed.date),
				requestId: parsed.requestId,
				responseChannel: parsed.responseChannel
			});

			await pub.publish(
				parsed.responseChannel,
				JSON.stringify({
					data: response,
					date: new Date().toISOString(),
					id: messageId++
				})
			);
		} catch (err) {
			console.error(`[RedisQuery:${channel}] handler error`, err);
		}
	});
};

module.exports = {
	query,
	queryListen
};
