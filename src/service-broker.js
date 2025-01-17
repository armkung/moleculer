/*
 * moleculer
 * Copyright (c) 2018 MoleculerJS (https://github.com/moleculerjs/moleculer)
 * MIT Licensed
 */

"use strict";

const Promise 				= require("bluebird");
const EventEmitter2 		= require("eventemitter2").EventEmitter2;
const _ 					= require("lodash");
const glob 					= require("glob");
const path 					= require("path");
const fs 					= require("fs");

const Transit 				= require("./transit");
const Registry 				= require("./registry");
const E 					= require("./errors");
const utils 				= require("./utils");
const Logger 				= require("./logger");
const Validator 			= require("./validator");

const Cachers 				= require("./cachers");
const Transporters 			= require("./transporters");
const Serializers 			= require("./serializers");
const Middlewares			= require("./middlewares");
const H 					= require("./health");
const MiddlewareHandler		= require("./middleware");
const cpuUsage 				= require("./cpu-usage");

/**
 * Default broker options
 */
const defaultOptions = {
	namespace: "",
	nodeID: null,

	logger: null,
	logLevel: null,
	logFormatter: "default",
	logObjectPrinter: null,

	transporter: null, //"TCP",

	requestTimeout: 0 * 1000,
	retryPolicy: {
		enabled: false,
		retries: 5,
		delay: 100,
		maxDelay: 1000,
		factor: 2,
		check: err => err && !!err.retryable
	},

	maxCallLevel: 0,
	heartbeatInterval: 5,
	heartbeatTimeout: 15,

	tracking: {
		enabled: false,
		shutdownTimeout: 5000,
	},

	disableBalancer: false,

	registry: {
		strategy: "RoundRobin",
		preferLocal: true
	},

	circuitBreaker: {
		enabled: false,
		threshold: 0.5,
		windowTime: 60,
		minRequestCount: 20,
		halfOpenTime: 10 * 1000,
		check: err => err && err.code >= 500
	},

	bulkhead: {
		enabled: false,
		concurrency: 10,
		maxQueueSize: 100,
	},

	transit: {
		maxQueueSize: 50 * 1000, // 50k ~ 400MB,
		packetLogFilter: [],
		disableReconnect: false,
		disableVersionCheck: false
	},

	cacher: null,
	serializer: null,

	validation: true,
	validator: null,
	metrics: false,
	metricsRate: 1,
	internalServices: true,
	internalMiddlewares: true,

	hotReload: false,

	middlewares: null,

	replCommands: null,

	// ServiceFactory: null,
	// ContextFactory: null
};

/**
 * Service broker class
 *
 * @class ServiceBroker
 */
class ServiceBroker {

	/**
	 * Creates an instance of ServiceBroker.
	 *
	 * @param {Object} options
	 *
	 * @memberof ServiceBroker
	 */
	constructor(options) {
		try {
			this.options = _.defaultsDeep(options, defaultOptions);

			// Promise constructor
			this.Promise = Promise;

			// Broker started flag
			this.started = false;

			// Class factories
			this.ServiceFactory = this.options.ServiceFactory || require("./service");
			this.ContextFactory = this.options.ContextFactory || require("./context");

			// Namespace
			this.namespace = this.options.namespace || "";

			// Self nodeID
			this.nodeID = this.options.nodeID || utils.getNodeID();

			// Logger
			this.logger = this.getLogger("broker");

			this.logger.info(`Moleculer v${this.MOLECULER_VERSION} is starting...`);
			this.logger.info(`Node ID: ${this.nodeID}`);
			this.logger.info(`Namespace: ${this.namespace || "<not defined>"}`);

			// Internal event bus
			this.localBus = new EventEmitter2({
				wildcard: true,
				maxListeners: 100
			});

			// Internal maps
			this.services = [];

			// Service registry
			this.registry = new Registry(this);

			// Middleware handler
			this.middlewares = new MiddlewareHandler(this);

			// Cacher
			this.cacher = Cachers.resolve(this.options.cacher);
			if (this.cacher) {
				this.cacher.init(this);

				const name = this.cacher.constructor.name;
				this.logger.info(`Cacher: ${name}`);
			}

			// Serializer
			this.serializer = Serializers.resolve(this.options.serializer);
			this.serializer.init(this);

			const serializerName = this.serializer.constructor.name;
			this.logger.info(`Serializer: ${serializerName}`);

			// Validation
			if (this.options.validation !== false) {
				this.validator = this.options.validator ? this.options.validator : new Validator();
				if (this.validator) {
					this.validator.init(this);
				}
			}

			// Transit & Transporter
			if (this.options.transporter) {
				const tx = Transporters.resolve(this.options.transporter);
				this.transit = new Transit(this, tx, this.options.transit);

				const txName = tx.constructor.name;
				this.logger.info(`Transporter: ${txName}`);

				if (this.options.disableBalancer) {
					if (tx.hasBuiltInBalancer) {
						this.logger.info("The broker built-in balancer is DISABLED.");
					} else {
						this.logger.warn(`The ${txName} has no built-in balancer. Broker balancer is ENABLED.`);
						this.options.disableBalancer = false;
					}
				}
			}

			if (this.options.disableBalancer) {
				this.call = this.callWithoutBalancer;
			}

			// Register middlewares
			this.registerMiddlewares(this.options.middlewares);

			// Register internal actions
			if (this.options.internalServices)
				this.registerInternalServices();

			this.middlewares.callSyncHandlers("created", [this]);

			// Call `created` event handler
			if (_.isFunction(this.options.created))
				this.options.created(this);

			// Graceful exit
			this._closeFn = () => {
				/* istanbul ignore next */
				this.stop()
					.catch(err => this.logger.error(err))
					.then(() => process.exit(0));
			};

			process.setMaxListeners(0);
			if ((this.options.skipProcessEventRegistration || false) !== true) {
				process.on("beforeExit", this._closeFn);
				process.on("exit", this._closeFn);
				process.on("SIGINT", this._closeFn);
				process.on("SIGTERM", this._closeFn);
			}
		} catch(err) {
			if (this.logger)
				this.fatal("Unable to create ServiceBroker.", err, true);
			else {
				/* eslint-disable-next-line no-console */
				console.error("Unable to create ServiceBroker.", err);
			}
		}

	}

	/**
	 * Register middlewares (user & internal)
	 *
	 * @memberof ServiceBroker
	 */
	registerMiddlewares(userMiddlewares) {
		// Register user middlewares
		if (Array.isArray(userMiddlewares) && userMiddlewares.length > 0) {
			userMiddlewares.forEach(mw => this.middlewares.add(mw));

			this.logger.info(`Registered ${this.middlewares.count()} custom middleware(s).`);
		}

		if (this.options.internalMiddlewares) {
			// Register internal middlewares
			const prevCount = this.middlewares.count();

			// 0. ActionHook
			this.middlewares.add(Middlewares.ActionHook.call(this));

			// 1. Validator
			if (this.validator && _.isFunction(this.validator.middleware))
				this.middlewares.add(this.validator.middleware());

			// 2. Bulkhead
			this.middlewares.add(Middlewares.Bulkhead.call(this));

			// 3. Cacher
			if (this.cacher && _.isFunction(this.cacher.middleware))
				this.middlewares.add(this.cacher.middleware());

			// 4. Context tracker
			this.middlewares.add(Middlewares.ContextTracker.call(this));

			// 5. CircuitBreaker
			this.middlewares.add(Middlewares.CircuitBreaker.call(this));

			// 6. Timeout
			this.middlewares.add(Middlewares.Timeout.call(this));

			// 7. Retry
			this.middlewares.add(Middlewares.Retry.call(this));

			// 8. Fallback
			this.middlewares.add(Middlewares.Fallback.call(this));

			// 9. Error handler
			this.middlewares.add(Middlewares.ErrorHandler.call(this));

			// 10. Metrics
			this.middlewares.add(Middlewares.Metrics.call(this));

			this.logger.info(`Registered ${this.middlewares.count() - prevCount} internal middleware(s).`);
		}

		this.middlewares.wrapBrokerMethods();
	}

	/**
	 * Start broker. If has transporter, transporter.connect will be called.
	 *
	 * @memberof ServiceBroker
	 */
	start() {
		return Promise.resolve()
			.then(() => {
				return this.middlewares.callHandlers("starting", [this]);
			})
			.then(() => {
				if (this.transit)
					return this.transit.connect();
			})
			.then(() => {
				// Call service `started` handlers
				return Promise.all(this.services.map(svc => svc._start.call(svc)));
			})
			.catch(err => {
				/* istanbul ignore next */
				this.logger.error("Unable to start all services.", err);
				return Promise.reject(err);
			})
			.then(() => {
				this.logger.info(`ServiceBroker with ${this.services.length} service(s) is started successfully.`);
				this.started = true;

				this.broadcastLocal("$broker.started");
			})
			.then(() => {
				if (this.transit)
					return this.transit.ready();
			})
			.then(() => {
				return this.middlewares.callHandlers("started", [this]);
			})
			.then(() => {
				if (_.isFunction(this.options.started))
					return this.options.started(this);
			});
	}

	/**
	 * Stop broker. If has transporter, transporter.disconnect will be called.
	 *
	 * @memberof ServiceBroker
	 */
	stop() {
		this.started = false;
		return Promise.resolve()
			.then(() => {
				if (this.transit) {
					this.registry.regenerateLocalRawInfo(true);
					// Send empty node info in order to block incoming requests
					return this.transit.sendNodeInfo();
				}
			})
			.then(() => {
				return this.middlewares.callHandlers("stopping", [this], true);
			})
			.then(() => {
				// Call service `stopped` handlers
				return Promise.all(this.services.map(svc => svc._stop.call(svc)));
			})
			.catch(err => {
				/* istanbul ignore next */
				this.logger.error("Unable to stop all services.", err);
			})
			.then(() => {
				if (this.transit) {
					return this.transit.disconnect();
				}
			})
			.then(() => {
				if (this.cacher) {
					return this.cacher.close();
				}
			})
			.then(() => {
				return this.middlewares.callHandlers("stopped", [this], true);
			})
			.then(() => {
				if (_.isFunction(this.options.stopped))
					return this.options.stopped(this);
			})
			.catch(err => {
				/* istanbul ignore next */
				this.logger.error(err);
			})
			.then(() => {
				this.logger.info("ServiceBroker is stopped. Good bye.");

				this.broadcastLocal("$broker.stopped");

				if ((this.options.skipProcessEventRegistration || false) !== true) {
					process.removeListener("beforeExit", this._closeFn);
					process.removeListener("exit", this._closeFn);
					process.removeListener("SIGINT", this._closeFn);
					process.removeListener("SIGTERM", this._closeFn);
				}
			});
	}

	/**
	 * Switch the console to REPL mode.
	 *
	 * @example
	 * broker.start().then(() => broker.repl());
	 *
	 * @memberof ServiceBroker
	 */
	repl() {
		let repl;
		try {
			repl = require("moleculer-repl");
		}
		catch (error) {
			console.error("The 'moleculer-repl' package is missing. Please install it with 'npm install moleculer-repl' command."); // eslint-disable-line no-console
			this.logger.error("The 'moleculer-repl' package is missing. Please install it with 'npm install moleculer-repl' command.");
			this.logger.debug("ERROR", error);
			return;
		}

		if (repl)
			repl(this, this.options.replCommands);
	}

	/**
	 * Get a custom logger for sub-modules (service, transporter, cacher, context...etc)
	 *
	 * @param {String} module	Name of module
	 * @param {String|object} props	Module properties (service name, version, ...etc
	 * @returns {Logger}
	 *
	 * @memberof ServiceBroker
	 */
	getLogger(module, props) {
		if (_.isString(props))
			props = { mod: props };

		let bindings = Object.assign({
			nodeID: this.nodeID,
			ns: this.namespace,
			mod: module
		}, props);

		// Call logger creator
		if (_.isFunction(this.options.logger))
			return this.options.logger.call(this, bindings);

		// External logger
		if (_.isObject(this.options.logger) && this.options.logger !== console)
			return Logger.extend(this.options.logger);

		// Disable logging
		if (this.options.logger === false)
			return Logger.createDefaultLogger();

		// Create console logger
		return Logger.createDefaultLogger(console, bindings, this.options.logLevel || "info", this.options.logFormatter, this.options.logObjectPrinter);
	}

	/**
	 * Fatal error. Print the message to console and exit the process (if need)
	 *
	 * @param {String} message
	 * @param {Error?} err
	 * @param {boolean} [needExit=true]
	 *
	 * @memberof ServiceBroker
	 */
	fatal(message, err, needExit = true) {

		if (this.logger)
			this.logger.fatal(message, err);
		else
			console.error(message, err); // eslint-disable-line no-console

		if (needExit)
			process.exit(1);
	}

	/**
	 * Load services from a folder
	 *
	 * @param {string} [folder="./services"]		Folder of services
	 * @param {string} [fileMask="**\/*.service.js"]	Service filename mask
	 * @returns	{Number}							Number of found services
	 *
	 * @memberof ServiceBroker
	 */
	loadServices(folder = "./services", fileMask = "**/*.service.js") {
		this.logger.debug(`Search services in '${folder}/${fileMask}'...`);

		let serviceFiles;

		if (Array.isArray(fileMask))
			serviceFiles = fileMask.map(f => path.join(folder, f));
		else
			serviceFiles = glob.sync(path.join(folder, fileMask));

		if (serviceFiles)
			serviceFiles.forEach(filename => this.loadService(filename));

		return serviceFiles.length;
	}

	/**
	 * Load a service from file
	 *
	 * @param {string} 		Path of service
	 * @returns	{Service}	Loaded service
	 *
	 * @memberof ServiceBroker
	 */
	loadService(filePath) {
		let fName = path.resolve(filePath);
		let schema;

		this.logger.debug(`Load service '${path.basename(fName)}'...`);

		try {
			schema = require(fName);
		} catch (e) {
			this.logger.error(`Failed to load service '${fName}'`, e);
			throw e;
		}

		let svc;
		schema = this.normalizeSchemaConstructor(schema);
		if (this.ServiceFactory.isPrototypeOf(schema)) {
			// Service implementation
			svc = new schema(this);

			// If broker is started, call the started lifecycle event of service
			if (this.started)
				this._restartService(svc);

		} else if (_.isFunction(schema)) {
			// Function
			svc = schema(this);
			if (!(svc instanceof this.ServiceFactory)) {
				svc = this.createService(svc);
			} else {
				// If broker is started, call the started lifecycle event of service
				if (this.started)
					this._restartService(svc);
			}

		} else if (schema) {
			// Schema object
			svc = this.createService(schema);
		}

		if (svc) {
			svc.__filename = fName;
		}

		if (this.options.hotReload) {
			this.watchService(svc || { __filename: fName, name: fName });
		}

		return svc;
	}

	/**
	 * Watch a service file and hot reload if it's changed.
	 *
	 * @param {Service} service
	 * @memberof ServiceBroker
	 */
	watchService(service) {
		if (service.__filename) {
			const debouncedHotReload = _.debounce(this.hotReloadService.bind(this), 500);

			this.logger.debug(`Watching '${service.name}' service file...`);

			// Better: https://github.com/paulmillr/chokidar
			const watcher = fs.watch(service.__filename, (eventType, filename) => {
				this.logger.info(`The ${filename} is changed. (Type: ${eventType})`);

				watcher.close();
				debouncedHotReload(service);
			});
		}
	}

	/**
	 * Hot reload a service
	 *
	 * @param {Service} service
	 * @returns {Service} Reloaded service instance
	 *
	 * @memberof ServiceBroker
	 */
	hotReloadService(service) {
		this.logger.info(`Hot reload '${service.name}' service...`, service.__filename);

		utils.clearRequireCache(service.__filename);

		return this.destroyService(service)
			.then(() => this.loadService(service.__filename));
	}

	/**
	 * Create a new service by schema
	 *
	 * @param {any} schema	Schema of service or a Service class
	 * @param {any=} schemaMods	Modified schema
	 * @returns {Service}
	 *
	 * @memberof ServiceBroker
	 */
	createService(schema, schemaMods) {
		let service;

		schema = this.normalizeSchemaConstructor(schema);
		if (this.ServiceFactory.isPrototypeOf(schema)) {
			service = new schema(this, schemaMods);
		} else {
			let s = schema;
			if (schemaMods)
				s = this.ServiceFactory.mergeSchemas(schema, schemaMods);

			service = new this.ServiceFactory(this, s);
		}

		// If broker is started, call the started lifecycle event of service
		if (this.started)
			this._restartService(service);

		return service;
	}

	/**
	 * Restart a hot-reloaded service after creation.
	 *
	 * @param {Service} service
	 * @returns {Promise}
	 * @memberof ServiceBroker
	 * @private
	 */
	_restartService(service) {
		return service._start.call(service)
			.catch(err => this.logger.error("Unable to start service.", err));
	}

	/**
	 * Add a local service instance
	 *
	 * @param {Service} service
	 * @memberof ServiceBroker
	 */
	addLocalService(service) {
		this.services.push(service);
	}

	/**
	 * Register a local service to Service Registry
	 *
	 * @param {Object} registryItem
	 * @memberof ServiceBroker
	 */
	registerLocalService(registryItem) {
		this.registry.registerLocalService(registryItem);
	}

	/**
	 * Destroy a local service
	 *
	 * @param {Service} service
	 * @memberof ServiceBroker
	 */
	destroyService(service) {
		return Promise.resolve()
			.then(() => service._stop())
			.catch(err => {
				/* istanbul ignore next */
				this.logger.error(`Unable to stop '${service.name}' service.`, err);
			})
			.then(() => {
				_.remove(this.services, svc => svc == service);
				this.registry.unregisterService(service.name, service.version);

				this.logger.info(`Service '${service.name}' is stopped.`);
				this.servicesChanged(true);

				return Promise.resolve();
			});
	}

	/**
	 * It will be called when a new local or remote service
	 * is registered or unregistered.
	 *
	 * @memberof ServiceBroker
	 */
	servicesChanged(localService = false) {
		this.broadcastLocal("$services.changed", { localService });

		// Should notify remote nodes, because our service list is changed.
		if (this.started && localService && this.transit) {
			this.transit.sendNodeInfo();
		}
	}

	/**
	 * Register internal services
	 *
	 * @memberof ServiceBroker
	 */
	registerInternalServices() {
		this.createService(require("./internals")(this));
	}

	/**
	 * Get a local service by name
	 *
	 * @param {String} name
	 * @param {String|Number} version
	 * @returns {Service}
	 *
	 * @memberof ServiceBroker
	 */
	getLocalService(name, version) {
		return this.services.find(service => service.name == name && service.version == version);
	}

	/**
	 * Wait for other services
	 *
	 * @param {String|Array<String>} serviceNames
	 * @param {Number} timeout Timeout in milliseconds
	 * @param {Number} interval Check interval in milliseconds
	 * @returns {Promise}
	 *
	 * @memberof ServiceBroker
	 */
	waitForServices(serviceNames, timeout, interval, logger = this.logger) {
		if (!Array.isArray(serviceNames))
			serviceNames = [serviceNames];

		const serviceObjs = serviceNames.map(x => {
			if (_.isPlainObject(x)) {
				return x;
			}

			if (_.isString(x)) {
				// Parse versioned service identifier strings
				const split = x.split(".");
				if (
					split.length === 2 &&
					split[0].length > 0 &&
					split[0].match(/^v\d+$/)
				) {
					return {
						name: split[1],
						version: Number(split[0].slice(1)),
					};
				}
				// If not versioned, fall back to the existing default action to hopefully avoid breaking existing implementations
			}

			return { name: x };
		}).filter(x => x.name);

		if (serviceObjs.length == 0)
			return Promise.resolve();

		logger.info(`Waiting for service(s) '${_.map(serviceObjs, "name").join(", ")}'...`);

		const startTime = Date.now();
		return new Promise((resolve, reject) => {
			const check = () => {
				const count = serviceObjs.filter(svcObj => {
					return this.registry.hasService(svcObj.name, svcObj.version);
				});

				if (count.length == serviceObjs.length) {
					logger.info(`Service(s) '${_.map(serviceObjs, "name").join(", ")}' are available.`);
					return resolve();
				}

				logger.debug(`${count.length} of ${serviceObjs.length} services are available. Waiting further...`);

				if (timeout && Date.now() - startTime > timeout)
					return reject(new E.MoleculerServerError("Services waiting is timed out.", 500, "WAITFOR_SERVICES", { services: serviceNames }));

				setTimeout(check, interval || 1000);
			};

			check();
		});
	}

	/**
	 * Add a middleware to the broker
	 *
	 * @param {Function} mws
	 *
	 * @deprecated
	 * @memberof ServiceBroker
	 */
	use(...mws) {
		utils.deprecate("The 'broker.use()' has been deprecated since v0.13. Use 'middlewares: [...]' in broker options instead.");
		mws.forEach(mw => this.middlewares.add(mw));
	}

	/**
	 * Find the next available endpoint for action
	 *
	 * @param {String} actionName
	 * @param {Object?} opts
	 * @returns {Endpoint|Error}
	 *
	 * @performance-critical
	 * @memberof ServiceBroker
	 */
	findNextActionEndpoint(actionName, opts) {
		if (typeof actionName !== "string") {
			return actionName;
		} else {
			if (opts && opts.nodeID) {
				const nodeID = opts.nodeID;
				// Direct call
				const endpoint = this.registry.getActionEndpointByNodeId(actionName, nodeID);
				if (!endpoint) {
					this.logger.warn(`Service '${actionName}' is not found on '${nodeID}' node.`);
					return new E.ServiceNotFoundError({ action: actionName, nodeID });
				}
				return endpoint;

			} else {
				// Get endpoint list by action name
				const epList = this.registry.getActionEndpoints(actionName);
				if (!epList) {
					this.logger.warn(`Service '${actionName}' is not registered.`);
					return new E.ServiceNotFoundError({ action: actionName });
				}

				// Get the next available endpoint
				const endpoint = epList.next(opts);
				if (!endpoint) {
					const errMsg = `Service '${actionName}' is not available.`;
					this.logger.warn(errMsg);
					return new E.ServiceNotAvailableError({ action: actionName });
				}
				return endpoint;
			}
		}
	}

	/**
	 * Call an action
	 *
	 * @param {String} actionName	name of action
	 * @param {Object?} params		params of action
	 * @param {Object?} opts		options of call (optional)
	 * @returns {Promise}
	 *
	 * @performance-critical
	 * @memberof ServiceBroker
	 */
	call(actionName, params, opts = {}) {
		const endpoint = this.findNextActionEndpoint(actionName, opts);
		if (endpoint instanceof Error)
			return Promise.reject(endpoint);

		// Create context
		let ctx;
		if (opts.ctx != null) {
			// Reused context
			ctx = opts.ctx;
			ctx.endpoint = endpoint;
			ctx.nodeID = endpoint.id;
			ctx.action = endpoint.action;
		} else {
			// New root context
			ctx = this.ContextFactory.create(this, endpoint, params, opts);
		}

		if (ctx.endpoint.local)
			this.logger.debug("Call action locally.", { action: ctx.action.name, requestID: ctx.requestID });
		else
			this.logger.debug("Call action on remote node.", { action: ctx.action.name, nodeID: ctx.nodeID, requestID: ctx.requestID });

		let p = endpoint.action.handler(ctx);

		// Pointer to Context
		p.ctx = ctx;

		return p;
	}

	/**
	 * Call an action without built-in balancer.
	 * You don't call it directly. Broker will replace the
	 * original 'call' method to this if you disable the
	 * built-in balancer with the "disableBalancer" option.
	 *
	 * @param {String} actionName	name of action
	 * @param {Object?} params		params of action
	 * @param {Object?} opts 		options of call (optional)
	 * @returns {Promise}
	 *
	 * @private
	 * @memberof ServiceBroker
	 */
	callWithoutBalancer(actionName, params, opts = {}) {
		let nodeID = null;
		let endpoint = null;
		if (typeof actionName !== "string") {
			endpoint = actionName;
			actionName = endpoint.action.name;
			nodeID = endpoint.id;
		} else {
			if (opts.nodeID) {
				nodeID = opts.nodeID;
				endpoint = this.registry.getActionEndpointByNodeId(actionName, nodeID);
				if (!endpoint) {
					this.logger.warn(`Service '${actionName}' is not found on '${nodeID}' node.`);
					return Promise.reject(new E.ServiceNotFoundError({ action: actionName, nodeID }));
				}
			} else {
				// Get endpoint list by action name
				const epList = this.registry.getActionEndpoints(actionName);
				if (epList == null) {
					this.logger.warn(`Service '${actionName}' is not registered.`);
					return Promise.reject(new E.ServiceNotFoundError({ action: actionName }));
				}

				endpoint = epList.getFirst();
				if (endpoint == null) {
					const errMsg = `Service '${actionName}' is not available.`;
					this.logger.warn(errMsg);
					return Promise.reject(new E.ServiceNotAvailableError({ action: actionName }));
				}
			}
		}

		// Create context
		let ctx;
		if (opts.ctx != null) {
			// Reused context
			ctx = opts.ctx;
			if (endpoint) {
				ctx.endpoint = endpoint;
				ctx.action = endpoint.action;
			}
		} else {
			// New root context
			ctx = this.ContextFactory.create(this, endpoint, params, opts);
		}
		ctx.nodeID = nodeID;

		this.logger.debug("Call action on a node.", { action: ctx.action.name, nodeID: ctx.nodeID, requestID: ctx.requestID });

		let p = endpoint.action.remoteHandler(ctx);

		// Pointer to Context
		p.ctx = ctx;

		return p;
	}

	_getLocalActionEndpoint(actionName) {
		// Find action by name
		let epList = this.registry.getActionEndpoints(actionName);
		if (epList == null || !epList.hasLocal()) {
			this.logger.warn(`Service '${actionName}' is not registered locally.`);
			throw new E.ServiceNotFoundError({ action: actionName, nodeID: this.nodeID });
		}

		// Get local endpoint
		let endpoint = epList.nextLocal();
		if (!endpoint) {
			this.logger.warn(`Service '${actionName}' is not available locally.`);
			throw new E.ServiceNotAvailableError({ action: actionName, nodeID: this.nodeID });
		}

		return endpoint;
	}

	/**
	 * Multiple action calls.
	 *
	 * @param {Array<Object>|Object} def Calling definitions.
	 * @returns {Promise<Array<Object>|Object>}
	 *
	 * @example
	 * Call `mcall` with an array:
	 * ```js
	 * broker.mcall([
	 * 	{ action: "posts.find", params: { limit: 5, offset: 0 } },
	 * 	{ action: "users.find", params: { limit: 5, sort: "username" }, opts: { timeout: 500 } }
	 * ]).then(results => {
	 * 	let posts = results[0];
	 * 	let users = results[1];
	 * })
	 * ```
	 *
	 * @example
	 * Call `mcall` with an Object:
	 * ```js
	 * broker.mcall({
	 * 	posts: { action: "posts.find", params: { limit: 5, offset: 0 } },
	 * 	users: { action: "users.find", params: { limit: 5, sort: "username" }, opts: { timeout: 500 } }
	 * }).then(results => {
	 * 	let posts = results.posts;
	 * 	let users = results.users;
	 * })
	 * ```
	 * @throws MoleculerServerError - If the `def` is not an `Array` and not an `Object`.
	 * @memberof ServiceBroker
	 */
	mcall(def) {
		if (Array.isArray(def)) {
			return Promise.all(def.map(item => this.call(item.action, item.params, item.options)));

		} else if (_.isObject(def)) {
			let results = {};
			return Promise.all(Object.keys(def).map(name => {
				const item = def[name];
				return this.call(item.action, item.params, item.options).then(res => results[name] = res);
			})).then(() => results);
		} else {
			throw new E.MoleculerServerError("Invalid calling definition.", 500, "INVALID_PARAMETERS");
		}
	}


	/**
	 * Emit an event (grouped & balanced global event)
	 *
	 * @param {string} eventName
	 * @param {any} payload
	 * @param {String|Array<String>=} groups
	 * @returns
	 *
	 * @memberof ServiceBroker
	 */
	emit(eventName, payload, groups) {
		if (groups && !Array.isArray(groups))
			groups = [groups];

		this.logger.debug(`Emit '${eventName}' event`+ (groups ? ` to '${groups.join(", ")}' group(s)` : "") + ".");

		// Call local/internal subscribers
		if (/^\$/.test(eventName))
			this.localBus.emit(eventName, payload);

		if (!this.options.disableBalancer) {

			const endpoints = this.registry.events.getBalancedEndpoints(eventName, groups);

			// Grouping remote events (reduce the network traffic)
			const groupedEP = {};

			endpoints.forEach(([ep, group]) => {
				if (ep) {
					if (ep.id == this.nodeID) {
						// Local service, call handler
						this.registry.events.callEventHandler(ep.event.handler, payload, this.nodeID, eventName);
					} else {
						// Remote service
						const e = groupedEP[ep.id];
						if (e)
							e.push(group);
						else
							groupedEP[ep.id] = [group];
					}
				} else {
					if (groupedEP[null])
						groupedEP[null].push(group);
					else
						groupedEP[null] = [group];
				}
			});

			if (this.transit) {
				// Remote service
				return this.transit.sendBalancedEvent(eventName, payload, groupedEP);
			}

		} else if (this.transit) {
			// Disabled balancer case

			if (!groups || groups.length == 0) {
				// Apply to all groups
				groups = this.getEventGroups(eventName);
			}

			if (groups.length == 0)
				return;

			return this.transit.sendEventToGroups(eventName, payload, groups);
		}
	}

	/**
	 * Broadcast an event for all local & remote services
	 *
	 * @param {string} eventName
	 * @param {any} payload
	 * @param {String|Array<String>=} groups
	 * @returns
	 *
	 * @memberof ServiceBroker
	 */
	broadcast(eventName, payload, groups = null) {
		if (groups && !Array.isArray(groups))
			groups = [groups];

		this.logger.debug(`Broadcast '${eventName}' event`+ (groups ? ` to '${groups.join(", ")}' group(s)` : "") + ".");

		if (this.transit) {
			if (!this.options.disableBalancer) {
				const endpoints = this.registry.events.getAllEndpoints(eventName, groups);

				// Send to remote services
				endpoints.forEach(ep => {
					if (ep.id != this.nodeID) {
						return this.transit.sendBroadcastEvent(ep.id, eventName, payload, groups);
					}
				});
			} else {
				// Disabled balancer case
				if (!groups || groups.length == 0) {
					// Apply to all groups
					groups = this.getEventGroups(eventName);
				}

				if (groups.length == 0)
					return;

				return this.transit.sendBroadcastEvent(null, eventName, payload, groups);
			}
		}

		// Send to local services
		return this.broadcastLocal(eventName, payload, groups);
	}

	/**
	 * Broadcast an event for all local services
	 *
	 * @param {string} eventName
	 * @param {any} payload
	 * @param {Array<String>?} groups
	 * @param {String?} nodeID
	 * @returns
	 *
	 * @memberof ServiceBroker
	 */
	broadcastLocal(eventName, payload, groups = null) {
		if (groups && !Array.isArray(groups))
			groups = [groups];

		this.logger.debug(`Broadcast '${eventName}' local event`+ (groups ? ` to '${groups.join(", ")}' group(s)` : "") + ".");

		// Call internal subscribers
		if (/^\$/.test(eventName))
			this.localBus.emit(eventName, payload);

		return this.emitLocalServices(eventName, payload, groups, this.nodeID, true);
	}

	/**
	 * Send ping to a node (or all nodes if nodeID is null)
	 *
	 * @param {String|Array<String>?} nodeID
	 * @param {Number?} timeout
	 * @returns {Promise}
	 * @memberof ServiceBroker
	 */
	ping(nodeID, timeout = 2000) {
		if (this.transit && this.transit.connected) {
			if (_.isString(nodeID)) {
				// Ping a single node
				return new Promise(resolve => {

					const timer = setTimeout(() => {
						this.localBus.off("$node.pong", handler);
						resolve(null);
					}, timeout);

					const handler = pong => {
						if (pong.nodeID == nodeID) {
							clearTimeout(timer);
							this.localBus.off("$node.pong", handler);
							resolve(pong);
						}
					};

					this.localBus.on("$node.pong", handler);

					this.transit.sendPing(nodeID);
				});

			} else {
				const pongs = {};
				let nodes = nodeID;
				if (!nodes) {
					nodes = this.registry.getNodeList({ onlyAvailable: true })
						.filter(node => node.id != this.nodeID)
						.map(node => node.id);
				}

				nodes.forEach(id => pongs[id] = null);
				const processing = new Set(nodes);

				// Ping multiple nodes
				return new Promise(resolve => {

					const timer = setTimeout(() => {
						this.localBus.off("$node.pong", handler);
						resolve(pongs);
					}, timeout);

					const handler = pong => {
						pongs[pong.nodeID] = pong;
						processing.delete(pong.nodeID);

						if (processing.size == 0) {
							clearTimeout(timer);
							this.localBus.off("$node.pong", handler);
							resolve(pongs);
						}
					};

					this.localBus.on("$node.pong", handler);

					nodes.forEach(id => this.transit.sendPing(id));
				});
			}
		}

		return this.Promise.resolve(nodeID ? null : []);
	}

	/**
	 * Get local node health status
	 *
	 * @returns {Promise}
	 * @memberof ServiceBroker
	 */
	getHealthStatus() {
		return H.getHealthStatus(this);
	}

	/**
	 * Get local node info.
	 *
	 * @returns
	 * @memberof ServiceBroker
	 */
	getLocalNodeInfo() {
		return this.registry.getLocalNodeInfo();
	}

	/**
	 * Get event groups by event name
	 *
	 * @param {String} eventName
	 * @returns
	 * @memberof ServiceBroker
	 */
	getEventGroups(eventName) {
		return this.registry.events.getGroups(eventName);
	}

	/**
	 * Emit event to local nodes. It is called from transit when a remote event received
	 * or from `broadcastLocal`
	 *
	 * @param {String} event
	 * @param {any} payload
	 * @param {any} groups
	 * @param {String} sender
	 * @param {boolean} broadcast
	 * @returns
	 * @memberof ServiceBroker
	 */
	emitLocalServices(event, payload, groups, sender, broadcast) {
		return this.registry.events.emitLocalServices(event, payload, groups, sender, broadcast);
	}

	/**
	 * Get node overall CPU usage
	 *
	 * @returns {Promise<object>}
	 * @memberof ServiceBroker
	 */
	getCpuUsage() {
		return cpuUsage();
	}


	/**
	 * Get the Constructor name of any object if it exists
	 * @param {any} obj
	 * @returns {string}
	 *
	 */
	getConstructorName(obj) {
		let target = obj.prototype;
		if (target && target.constructor && target.constructor.name){
			return target.constructor.name;
		}
		if (obj.constructor && obj.constructor.name){
			return obj.constructor.name;
		}
		return undefined;
	}

	/**
	 * Ensure the service schema will be prototype of ServiceFactory;
	 *
	 * @param {any} schema
	 * @returns {string}
	 *
	 */
	normalizeSchemaConstructor(schema) {
		if (this.ServiceFactory.isPrototypeOf(schema)){
			return schema;
		}
		// Sometimes the schame was loaded from another node_module or is a object copy.
		// Then we will check if the constructor name is the same, asume that is a derivate object
		// and adjust the prototype of the schema.
		let serviceName = this.getConstructorName(this.ServiceFactory);
		let target = this.getConstructorName(schema);
		if (serviceName === target){
			Object.setPrototypeOf(schema, this.ServiceFactory);
			return schema;
		}
		// Depending how the schema was create the correct constructor name (from base class) will be locate on __proto__.
		target = this.getConstructorName(schema.__proto__);
		if (serviceName === target){
			Object.setPrototypeOf(schema.__proto__, this.ServiceFactory);
			return schema;
		}
		// This is just to handle some idiosyncrasies from Jest.
		if (schema._isMockFunction){
			target = this.getConstructorName(schema.prototype.__proto__);
			if (serviceName === target){
				Object.setPrototypeOf(schema, this.ServiceFactory);
				return schema;
			}
		}
		return schema;
	}
}

/**
 * Version of Moleculer
 */
ServiceBroker.MOLECULER_VERSION = require("../package.json").version;
ServiceBroker.prototype.MOLECULER_VERSION = ServiceBroker.MOLECULER_VERSION;

/**
 * Version of Protocol
 */
ServiceBroker.PROTOCOL_VERSION = "3";
ServiceBroker.prototype.PROTOCOL_VERSION = ServiceBroker.PROTOCOL_VERSION;

/**
 * Default configuration
 */
ServiceBroker.defaultOptions = defaultOptions;

module.exports = ServiceBroker;
