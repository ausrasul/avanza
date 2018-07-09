'use strict';

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var EventEmitter = require('events');
var https = require('https');
var querystring = require('querystring');
var WebSocket = require('ws');

var constants = require('./constants');
var totp = require('./totp');

// eslint-disable-next-line import/newline-after-import
var VERSION = require('../package.json').version;
var BASE_URL = 'www.avanza.se';
var USER_AGENT = process.env.AVANZA_USER_AGENT || 'Avanza API client/' + VERSION;
var MIN_INACTIVE_MINUTES = 30;
var MAX_INACTIVE_MINUTES = 60 * 24;
var SOCKET_URL = 'wss://www.avanza.se/_push/cometd';
var MAX_BACKOFF_MS = 2 * 60 * 1000;

/**
 * Simple debug utility function
 *
 * @private
 * @param {String} message The message to log
 */
function debug() {
  if (process.env.NODE_ENV === 'development') {
    var _console;

    // eslint-disable-next-line no-console
    (_console = console).error.apply(_console, arguments);
  }
}

/**
* Execute a request.
*
* @private
* @param {Object} options Request options.
* @return {Promise}
*/
function request(options) {
  if (!options) {
    return Promise.reject('Missing options.');
  }
  var data = JSON.stringify(options.data);
  return new Promise(function (resolve, reject) {
    var req = https.request({
      host: BASE_URL,
      port: 443,
      method: options.method,
      path: options.path,
      headers: Object.assign({
        'Accept': '*/*',
        'Content-Type': 'application/json',
        'User-Agent': USER_AGENT,
        'Content-Length': data.length
      }, options.headers)
    }, function (response) {
      var body = [];
      response.on('data', function (chunk) {
        return body.push(chunk);
      });
      response.on('end', function () {
        var parsedBody = body.join('');

        try {
          parsedBody = JSON.parse(parsedBody);
        } catch (e) {
          debug('Received non-JSON data from API.', body);
        }

        var res = {
          statusCode: response.statusCode,
          statusMessage: response.statusMessage,
          headers: response.headers,
          body: parsedBody
        };
        if (response.statusCode < 200 || response.statusCode > 299) {
          reject(res);
        } else {
          resolve(res);
        }
      });
    });
    if (data) {
      req.write(data);
    }
    req.on('error', function (e) {
      return reject(e);
    });
    req.end();
  });
}

/**
* An Avanza API wrapper.
*
* ### Constants
*
* Some methods require certain constants as parameters. These are described below.
*
* #### Instrument types
*
* | Type                          | Note |
* | :---------------------------- | :--- |
* | `Avanza.STOCK`                |      |
* | `Avanza.FUND`                 |      |
* | `Avanza.BOND`                 |      |
* | `Avanza.OPTION`               |      |
* | `Avanza.FUTURE_FORWARD`       |      |
* | `Avanza.CERTIFICATE`          |      |
* | `Avanza.WARRANT`              |      |
* | `Avanza.EXCHANGE_TRADED_FUND` |      |
* | `Avanza.INDEX`                |      |
* | `Avanza.PREMIUM_BOND`         |      |
* | `Avanza.SUBSCRIPTION_OPTION`  |      |
* | `Avanza.EQUITY_LINKED_BOND`   |      |
* | `Avanza.CONVERTIBLE`          |      |
*
* #### Periods
*
* | Period                | Note |
* | :-------------------- | :--- |
* | `Avanza.TODAY`        |      |
* | `Avanza.ONE_WEEK`     |      |
* | `Avanza.ONE_MONTH`    |      |
* | `Avanza.THREE_MONTHS` |      |
* | `Avanza.THIS_YEAR`    |      |
* | `Avanza.ONE_YEAR`     |      |
* | `Avanza.FIVE_YEARS`   |      |
*
* #### Lists
*
* | List                                              | Note |
* | :------------------------------------------------ | :--- |
* | `Avanza.HIGHEST_RATED_FUNDS`                      |      |
* | `Avanza.LOWEST_FEE_INDEX_FUNDS`                   |      |
* | `Avanza.BEST_DEVELOPMENT_FUNDS_LAST_THREE_MONTHS` |      |
* | `Avanza.MOST_OWNED_FUNDS`                         |      |
*
* #### Channels
*
* Note that for all channels where a _sequence_ of account IDs are expected
* (`<accountId1>,<accountId2>,...`), you must supply all of your account IDs,
* regardless of whether or not you want data for that account.
*
* | Channel                     | Note                                                                                                                |
* | :-------------------------- | :------------------------------------------------------------------------------------------------------------------ |
* | `Avanza.QUOTES`             | Minute-wise data containing current price, change, total volume traded etc. Expects an **orderbookId**.             |
* | `Avanza.ORDERDEPTHS`        | Best five offers and current total volume on each side. Expects an **orderbookId**.                                 |
* | `Avanza.TRADES`             | Updates whenever a new trade is made. Data contains volume, price, broker etc. Expects an **orderbookId**.          |
* | `Avanza.BROKERTRADESUMMARY` | Pushes data about which brokers are long/short and how big their current net volume is. Expects an **orderbookId**. |
* | `Avanza.POSITIONS`          | Your positions in an instrument. Expects a string of `<orderbookId>_<accountId1>,<accountId2,<accountId3>,...`.     |
* | `Avanza.ORDERS`             | Your current orders. Expects a string of `_<accountId1>,<accountId2,<accountId3>,...`.                              |
* | `Avanza.DEALS`              | Recent trades you have made. Expects a string of `_<accountId1>,<accountId2,<accountId3>,...`.                      |
* | `Avanza.ACCOUNTS`           | N/A. Expects a string of `_<accountId>`.                                                                            |
*
* #### Transaction Types
*
* | Transaction type          | Note |
* | :------------------------ | :--- |
* | `Avanza.OPTIONS`          |      |
* | `Avanza.FOREX`            |      |
* | `Avanza.DEPOSIT_WITHDRAW` |      |
* | `Avanza.BUY_SELL`         |      |
* | `Avanza.DIVIDEND`         |      |
* | `Avanza.INTEREST`         |      |
* | `Avanza.FOREIGN_TAX`      |      |
*
* #### Order Types
*
* | Order type    | Note |
* | :------------ | :--- |
* | `Avanza.BUY`  |      |
* | `Avanza.SELL` |      |
*
* @extends EventEmitter
*
*/

var Avanza = function (_EventEmitter) {
  _inherits(Avanza, _EventEmitter);

  function Avanza() {
    _classCallCheck(this, Avanza);

    var _this = _possibleConstructorReturn(this, (Avanza.__proto__ || Object.getPrototypeOf(Avanza)).call(this));

    _this._credentials = null;
    _this._socket = null;
    _this._authenticated = false;
    _this._authenticationSession = null;
    _this._authenticationTimeout = MAX_INACTIVE_MINUTES;
    _this._pushSubscriptionId = null;
    _this._reauthentication = null;
    _this._customerId = null;
    _this._securityToken = null;

    _this._backOffTimestamps = {};
    _this._socketHandshakeTimer = null;
    _this._socketSubscriptions = {};
    _this._socketMonitor = null;
    _this._socketLastMetaConnect = 0;
    _this._adviceTimeout = 30000;
    _this._socketConnected = false;
    _this._socketMessageCount = 1;
    _this._socketClientId = null;
    return _this;
  }

  /* Back off algoritm helper. Avoid accidental hammering when responding
   * to asynchronous events by scheduling the response using setTimeout()
   * with this function as the timeout input. Example:
   *   setTimeout(() => { ... }, _backoffCalc('relogin')) */


  _createClass(Avanza, [{
    key: '_backoffCalc',
    value: function _backoffCalc(actionName) {
      var now = Date.now();
      var schedDelay = 0;
      if (now - this._backOffTimestamps[actionName] < MAX_BACKOFF_MS * 5) {
        schedDelay = (now - this._backOffTimestamps[actionName]) * 2 + 500;
        if (schedDelay > MAX_BACKOFF_MS) {
          schedDelay = MAX_BACKOFF_MS;
          this._backOffTimestamps[actionName] = now;
        }
      } else {
        this._backOffTimestamps[actionName] = now;
      }
      return schedDelay;
    }
  }, {
    key: '_socketRestart',
    value: function _socketRestart() {
      var _this2 = this;

      this._socket.removeAllListeners();
      this._socket.on('error', function (err) {});
      this._socket.terminate();
      this._socketConnected = false;
      delete this._backOffTimestamps.handshake;
      clearInterval(this._socketMonitor);
      clearTimeout(this._socketHandshakeTimer);
      setTimeout(function () {
        _this2._socketInit(true);
      }, this._backoffCalc('websocket'));
    }
  }, {
    key: '_socketInit',
    value: function _socketInit(restart) {
      var _this3 = this;

      if (this._socket && !restart) {
        return;
      }

      this._socket = new WebSocket(SOCKET_URL);

      this._socket.on('open', function () {
        _this3._authenticateSocket();
      });
      this._socket.on('message', function (data) {
        _this3._socketHandleMessage(data);
      });
      this._socket.on('close', function () {
        _this3._socketRestart();
      });
      this._socket.on('error', function (err) {
        _this3._socketRestart();
      });

      this._socketMonitor = setInterval(function () {
        if (!_this3._pushSubscriptionId) {
          // Don't maintain socket status unless we're authenticated
          return;
        } else if (_this3._socket.readyState !== _this3._socket.OPEN) {
          // Don't make the assumption we will reach the open state
          // and hence don't assume there will ever be a close emitted.
          _this3._socketRestart();
        } else if (_this3._socketConnected && _this3._socketLastMetaConnect + _this3._adviceTimeout + 5000 < Date.now()) {
          _this3._socketRestart();
        }
      }, 5000);
    }
  }, {
    key: '_socketSend',
    value: function _socketSend(data) {
      if (this._socket && this._socket.readyState === this._socket.OPEN) {
        this._socket.send(JSON.stringify([data]));
        this._socketMessageCount += 1;
      }
    }
  }, {
    key: '_socketHandleMessage',
    value: function _socketHandleMessage(data) {
      var _this4 = this;

      var response = JSON.parse(data);
      for (var i = 0; i < response.length; i++) {
        if (!response[i]) {
          continue;
        }
        var message = response[i];
        if (message.error) {
          debug(message.error);
        }
        switch (message.channel) {
          case '/meta/disconnect':
            if (this._socketClientId) {
              this._authenticateSocket(true);
            }
            break;
          case '/meta/handshake':
            if (message.successful) {
              this._socketClientId = message.clientId;
              this._socketSend({
                advice: { timeout: 0 },
                channel: '/meta/connect',
                clientId: this._socketClientId,
                connectionType: 'websocket',
                id: this._socketMessageCount
              });
            } else if (message.advice && message.advice.reconnect === 'handshake') {
              this._authenticateSocket(true);
            } else {
              this._socketClientId = null;
              this._socketConnected = false;
              this._pushSubscriptionId = undefined;
              this._scheduleReauth();
            }
            break;
          case '/meta/connect':
            if (message.successful && (!message.advice || message.advice.reconnect !== 'none' && !(message.advice.interval < 0))) {
              this._socketLastMetaConnect = Date.now();
              this._socketSend({
                channel: '/meta/connect',
                clientId: this._socketClientId,
                connectionType: 'websocket',
                id: this._socketMessageCount
              });
              if (!this._socketConnected) {
                this._socketConnected = true;
                Object.keys(this._socketSubscriptions).forEach(function (substr) {
                  if (_this4._socketSubscriptions[substr] !== _this4._socketClientId) {
                    _this4._socketSubscribe(substr);
                  }
                });
              }
            } else if (this._socketClientId) {
              this._authenticateSocket(true);
            }
            break;
          case '/meta/subscribe':
            this._socketSubscriptions[message.subscription] = this._socketClientId;
            break;
          default:
            this.emit(message.channel, message.data);
        }
      }
    }
  }, {
    key: '_authenticateSocket',
    value: function _authenticateSocket(forceHandshake) {
      var _this5 = this;

      if (!this._socketClientId || forceHandshake) {
        this._socketClientId = null;
        this._socketConnected = false;
        if (this._pushSubscriptionId) {
          clearTimeout(this._socketHandshakeTimer);
          this._socketHandshakeTimer = setTimeout(function () {
            _this5._socketSend({
              advice: {
                timeout: 60000,
                interval: 0
              },
              channel: '/meta/handshake',
              ext: { subscriptionId: _this5._pushSubscriptionId },
              id: _this5._socketMessageCounter,
              minimumVersion: '1.0',
              supportedConnectionTypes: ['websocket', 'long-polling', 'callback-polling'],
              version: '1.0'
            });
          }, this._backoffCalc('handshake'));
        }
      } else if (this._socketClientId) {
        this._socketSend({
          channel: '/meta/connect',
          clientId: this._socketClientId,
          connectionType: 'websocket',
          id: this._socketMessageCount
        });
      }
    }
  }, {
    key: '_socketSubscribe',
    value: function _socketSubscribe(subscriptionString) {
      this._socketSubscriptions[subscriptionString] = null;
      if (this._socketConnected) {
        this._socketSend({
          channel: '/meta/subscribe',
          clientId: this._socketClientId,
          id: this._socketMessageCount,
          subscription: subscriptionString
        });
      }
    }

    /**
    * Authenticate the client.
    *
    * If second factor authentication is needed, either the one time code can be provided in `totp`, or the secret to
    * generate codes can be provided in `totpSecret`.
    *
    * @param {Object} credentials
    * @param {String} credentials.username
    * @param {String} credentials.password
    * @param {String} credentials.totp
    * @param {String} credentials.totpSecret
    */

  }, {
    key: 'authenticate',
    value: function authenticate(credentials) {
      var _this6 = this;

      if (!credentials) {
        return Promise.reject('Missing credentials.');
      }
      if (!credentials.username) {
        return Promise.reject('Missing credentials.username.');
      }
      if (!credentials.password) {
        return Promise.reject('Missing credentials.password.');
      }
      if (!(this._authenticationTimeout >= MIN_INACTIVE_MINUTES && this._authenticationTimeout <= MAX_INACTIVE_MINUTES)) {
        return Promise.reject('Session timeout not in range ' + MIN_INACTIVE_MINUTES + ' - ' + MAX_INACTIVE_MINUTES + ' minutes.');
      }

      return new Promise(function (resolve, reject) {
        var data = {
          maxInactiveMinutes: _this6._authenticationTimeout,
          password: credentials.password,
          username: credentials.username
        };
        request({
          method: 'POST',
          path: constants.paths.AUTHENTICATION_PATH,
          data: data
        }).then(function (response) {
          // No second factor requested, continue with normal login
          if (typeof response.body.twoFactorLogin === 'undefined') {
            return Promise.resolve(response);
          }
          var tfaOpts = response.body.twoFactorLogin;

          if (tfaOpts.method !== 'TOTP') {
            return Promise.reject('Unsupported second factor method ' + tfaOpts.method);
          }
          var totpCode = credentials.totpSecret ? totp(credentials.totpSecret) : credentials.totp;

          if (!totpCode) {
            return Promise.reject('Missing credentials.totp or credentials.totpSecret');
          }

          return request({
            method: 'POST',
            path: constants.paths.TOTP_PATH,
            data: {
              method: 'TOTP',
              totpCode: totpCode
            },
            headers: {
              Cookie: 'AZAMFATRANSACTION=' + tfaOpts.transactionId
            }
          });
        }).then(function (response) {
          _this6._authenticated = true;
          _this6._credentials = credentials;
          _this6._securityToken = response.headers['x-securitytoken'];
          _this6._authenticationSession = response.body.authenticationSession;
          _this6._pushSubscriptionId = response.body.pushSubscriptionId;
          _this6._customerId = response.body.customerId;

          // Re-authenticate after timeout minus one minute
          _this6._scheduleReauth((_this6._authenticationTimeout - 1) * 60 * 1000);

          if (_this6._socket) {
            _this6._socketRestart();
          }

          resolve({
            securityToken: _this6._securityToken,
            authenticationSession: _this6._authenticationSession,
            pushSubscriptionId: _this6._pushSubscriptionId,
            customerId: _this6._customerId
          });
        }).catch(function (e) {
          _this6._pushSubscriptionId = undefined;
          reject(e);
        });
      });
    }

    /* Re-authenticate after specified timeout.
     * In the event of failure retry with backoff until we succeed.
     */

  }, {
    key: '_scheduleReauth',
    value: function _scheduleReauth(delay) {
      var _this7 = this;

      clearTimeout(this._reauthentication);
      this._reauthentication = setTimeout(function () {
        _this7.authenticate(_this7._credentials).catch(function (error) {
          _this7._scheduleReauth(_this7._backoffCalc('authenticate'));
        });
      }, delay || this._backoffCalc('authenticate'));
    }

    /** Disconnects by simulating a client that just goes away. */

  }, {
    key: 'disconnect',
    value: function disconnect() {
      clearTimeout(this._reauthentication);
      this._authenticated = false; // Make sure all calls to main site will fail after this point

      this.removeAllListeners(); // Remove all subscription callbacks
      clearInterval(this._socketMonitor);
      if (this._socket) {
        this._socket.removeAllListeners();
        this._socket.on('error', function (err) {});
        this._socket.terminate();
        this._socket = null;
      }
      this._socketClientId = null;
      this._socketConnected = false;
      this._pushSubscriptionId = undefined;
      this._socketSubscriptions = {}; // Next startup of websocket should start without subscriptions
    }

    /**
    * Get all `positions` held by this user.
    */

  }, {
    key: 'getPositions',
    value: function getPositions() {
      return this.call('GET', constants.paths.POSITIONS_PATH);
    }

    /**
    * Get an overview of the users holdings at Avanza Bank.
    */

  }, {
    key: 'getOverview',
    value: function getOverview() {
      return this.call('GET', constants.paths.OVERVIEW_PATH);
    }

    /**
    * Get an overview of the users holdings for a specific account at Avanza Bank.
    * @param {String} accountId A valid account ID.
    *
    */

  }, {
    key: 'getAccountOverview',
    value: function getAccountOverview(accountId) {
      var path = constants.paths.ACCOUNT_OVERVIEW_PATH.replace('{0}', accountId);
      return this.call('GET', path);
    }

    /**
    * Get recent deals and orders.
    */

  }, {
    key: 'getDealsAndOrders',
    value: function getDealsAndOrders() {
      return this.call('GET', constants.paths.DEALS_AND_ORDERS_PATH);
    }

    /**
    * Get all transactions of an account.
    *
    * @param {String} accountOrTransactionType A valid account ID or a
    *                                          [Transaction Type](#transaction-type).
    * @param {Object} options Configuring which transactions to fetch.
    * @param {String} [options.from] On the form YYYY-MM-DD.
    * @param {String} [options.to] On the form YYYY-MM-DD.
    * @param {Number} [options.maxAmount] Only fetch transactions of at most this value.
    * @param {Number} [options.minAmount] Only fetch transactions of at least this value.
    * @param {String|Array} [options.orderbookId] Only fetch transactions involving
    *                                             this/these orderbooks.
    */

  }, {
    key: 'getTransactions',
    value: function getTransactions(accountOrTransactionType, options) {
      var path = constants.paths.TRANSACTIONS_PATH.replace('{0}', accountOrTransactionType);

      if (options && Array.isArray(options.orderbookId)) {
        options.orderbookId = options.orderbookId.join(',');
      }

      // Unsure what this is.
      // options.includeInstrumentsWithNoOrderbook = 1

      var query = querystring.stringify(options);
      return this.call('GET', query ? path + '?' + query : path);
    }

    /**
    * Get all watchlists created by this user. Note that the second table was
    * created from a specific watchlist, and so the response from the API will be
    * different for you.
    */

  }, {
    key: 'getWatchlists',
    value: function getWatchlists() {
      return this.call('GET', constants.paths.WATCHLISTS_PATH);
    }

    /**
    * Add an instrument to the watchlist.
    *
    * @param {String} instrumentId The ID of the instrument to add.
    * @param {String} watchlistId  The ID of the watchlist to add the instrument to.
    */

  }, {
    key: 'addToWatchlist',
    value: function addToWatchlist(instrumentId, watchlistId) {
      var path = constants.paths.WATCHLISTS_ADD_DELETE_PATH.replace('{0}', watchlistId).replace('{1}', instrumentId);
      return this.call('PUT', path);
    }

    /**
    * Remove an instrument from the watchlist.
    *
    * @param {String} instrumentId The ID of the instrument to remove.
    * @param {String} watchlistId  The ID of the watchlist to remove the instrument from.
    */

  }, {
    key: 'removeFromWatchlist',
    value: function removeFromWatchlist(instrumentId, watchlistId) {
      var path = constants.paths.WATCHLISTS_ADD_DELETE_PATH.replace('{0}', watchlistId).replace('{1}', instrumentId);
      return this.call('DELETE', path);
    }

    /**
    * Get instrument information.
    *
    * @param {String} instrumentId Likely the same as the instrumentId.
    * @param {String} instrumentType The type of the instrument. See
    *                                [Instrument Types](#instrument-types).
    */

  }, {
    key: 'getInstrument',
    value: function getInstrument(instrumentType, instrumentId) {
      var path = constants.paths.INSTRUMENT_PATH.replace('{0}', instrumentType.toLowerCase()).replace('{1}', instrumentId);
      return this.call('GET', path);
    }

    /**
    * Get orderbook information.
    *
    * @param {String} orderbookId Likely the same as the instrumentId.
    * @param {String} instrumentType The type of the instrument. See
    *                                [Instrument Types](#instrument-types).
    */

  }, {
    key: 'getOrderbook',
    value: function getOrderbook(instrumentType, orderbookId) {
      var path = constants.paths.ORDERBOOK_PATH.replace('{0}', instrumentType.toLowerCase());
      var query = querystring.stringify({ orderbookId: orderbookId });
      return this.call('GET', path + '?' + query);
    }

    /**
    * Get information about multiple orderbooks.
    *
    * @param {Array} orderbookIds A list of orderbook IDs.
    */

  }, {
    key: 'getOrderbooks',
    value: function getOrderbooks(orderbookIds) {
      var ids = orderbookIds.join(',');
      var path = constants.paths.ORDERBOOK_LIST_PATH.replace('{0}', ids);
      var query = querystring.stringify({ sort: 'name' });
      return this.call('GET', path + '?' + query);
    }

    /**
    * Get an array of prices over a period of time.
    *
    * @param {String} orderbookId The orderbook to fetch price data about.
    * @param {Period} period The period from which to fetch data. See [Periods](#periods).
    */

  }, {
    key: 'getChartdata',
    value: function getChartdata(orderbookId, period) {
      period = period.toLowerCase();
      var path = constants.paths.CHARTDATA_PATH.replace('{0}', orderbookId);
      var query = querystring.stringify({ timePeriod: period });
      return this.call('GET', path + '?' + query);
    }

    /**
    * List all inspiration lists.
    */

  }, {
    key: 'getInspirationLists',
    value: function getInspirationLists() {
      return this.call('GET', constants.paths.INSPIRATION_LIST_PATH.replace('{0}', ''));
    }

    /**
    * Get information about a single inspiration list.
    *
    * @param {String} list List type. See [Lists](#lists)
    */

  }, {
    key: 'getInspirationList',
    value: function getInspirationList(type) {
      return this.call('GET', constants.paths.INSPIRATION_LIST_PATH.replace('{0}', type));
    }

    /**
    * Subscribe to real-time data.
    *
    * @param {String} channel The channel on which to listen. See [Channels](#channels).
    * @param {String|Array} ids One or many IDs to subscribe to.
    * @param {Function} callback
    */

  }, {
    key: 'subscribe',
    value: function subscribe(channel, ids, callback) {
      if (!this._pushSubscriptionId) {
        throw new Error('Expected to be authenticated before subscribing.');
      }

      if (Array.isArray(ids)) {
        if (channel === Avanza.ORDERS || channel === Avanza.DEALS || channel === Avanza.POSITIONS) {
          ids = ids.join(',');
        } else {
          throw new Error('Channel ' + channel + ' does not support multiple ids as input.');
        }
      }

      if (!this._socket) {
        this._socketInit();
      }

      var subscriptionString = '/' + channel + '/' + ids;
      this.on(subscriptionString, function (data) {
        return callback(data);
      });
      this._socketSubscribe(subscriptionString);
    }

    /**
    * Place a limit order.
    *
    * @param {Object} options Order options.
    * @param {String} options.accountId ID of the account to trade on.
    * @param {String} options.orderbookId ID of the instrument to trade.
    * @param {String} options.orderType One of "BUY" or "SELL".
    * @param {Number} options.price The price limit of the order.
    * @param {String} options.validUntil A date on the form YYYY-MM-DD. Cancels
    *                                    the order if this date is passed.
    * @param {Number} options.volume How many securities to order.
    * @return {Object} Properties are `messages`, `requestId`, `status`, `orderId`.
    */

  }, {
    key: 'placeOrder',
    value: function placeOrder(options) {
      return this.call('POST', constants.paths.ORDER_PLACE_DELETE_PATH, options);
    }

    /**
     * Get information about an order.
     *
     * It is quite hard to automatically generate tables of what this endpoint
     * returns since orders are merely temporary entities.
     *
     * The returned object however looks very much like that from
     * [getOrderbook()](#getorderbook) with an extra property `order` which
     * contains information you already have (such as order price or volume).
     *
     * @param {String} instrumentType Instrument type of the pertaining instrument.
     *                                See [Instrument Types](#instrument-types).
     * @param {String} accountId ID of the account which this order was placed on.
     * @param {String} orderId ID of the order.
     */

  }, {
    key: 'getOrder',
    value: function getOrder(instrumentType, accountId, orderId) {
      var path = constants.paths.ORDER_GET_PATH.replace('{0}', instrumentType.toLowerCase());
      var query = querystring.stringify({ accountId: accountId, orderId: orderId });
      return this.call('GET', path + '?' + query);
    }

    /**
     * Edit an order.
     *
     * @param {String} instrumentType Instrument type of the pertaining instrument.
     *                                See [Instrument Types](#instrument-types).
     * @param {String} orderId Order ID received when placing the order.
     * @param {Object} options Order options. See [placeOrder()](#placeorder).
     */

  }, {
    key: 'editOrder',
    value: function editOrder(instrumentType, orderId, options) {
      options.orderCondition = 'NORMAL';
      var path = constants.paths.ORDER_EDIT_PATH.replace('{0}', instrumentType.toLowerCase()).replace('{1}', orderId);
      return this.call('PUT', path, options);
    }

    /**
    * Delete and cancel an order.
    *
    * @param {String} accountId ID of the account on which this order was placed.
    * @param {String} orderId Order ID received when the order was placed.
    */

  }, {
    key: 'deleteOrder',
    value: function deleteOrder(accountId, orderId) {
      var path = constants.paths.ORDER_PLACE_DELETE_PATH;
      var query = querystring.stringify({ accountId: accountId, orderId: orderId });
      return this.call('DELETE', path + '?' + query);
    }

    /**
    * Free text search for an instrument.
    *
    * @param {String} query Search query.
    * @param {String} [type] An instrument type.
    */

  }, {
    key: 'search',
    value: function search(searchQuery, type) {
      var path = void 0;

      if (type) {
        path = constants.paths.SEARCH_PATH.replace('{0}', type.toUpperCase());
      } else {
        path = constants.paths.SEARCH_PATH.replace('/{0}', '');
      }

      var query = querystring.stringify({
        limit: 100,
        query: searchQuery
      });

      return this.call('GET', path + '?' + query);
    }

    /**
    * Make a call to the API. Note that this method will filter dangling question
    * marks from `path`.
    *
    * @param {String} [method='GET'] HTTP method to use.
    * @param {String} [path=''] The URL to send the request to.
    * @param {Object} [data={}] JSON data to send with the request.
    * @return {Promise}
    */

  }, {
    key: 'call',
    value: function call() {
      var method = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : 'GET';

      var _this8 = this;

      var path = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : '';
      var data = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : {};

      var authenticationSession = this._authenticationSession;
      var securityToken = this._securityToken;

      // Remove dangling question mark
      if (path.slice(-1) === '?') {
        path = path.slice(0, -1);
      }

      return new Promise(function (resolve, reject) {
        if (!_this8._authenticated) {
          reject('Expected to be authenticated before calling.');
        } else {
          request({
            method: method,
            path: path,
            data: data,
            headers: {
              'X-AuthenticationSession': authenticationSession,
              'X-SecurityToken': securityToken
            }
          }).then(function (response) {
            return resolve(response.body);
          }).catch(function (e) {
            return reject(e);
          });
        }
      });
    }
  }]);

  return Avanza;
}(EventEmitter);

// Expose public constants


Object.keys(constants.public).forEach(function (key) {
  Object.defineProperty(Avanza, key, {
    value: constants.public[key]
  });
});

module.exports = Avanza;