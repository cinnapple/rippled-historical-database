var winston = require('winston');

var Log = function (scope) {
  var silent;
  var level = 4;
  var self = this;
  
  this.winston = new (require('winston').Logger)({
    transports: [new (winston.transports.Console)()]   
  });
  
  this.level = function (l) {
    level = parseInt(l, 10);
  };
  
  function log(type, args) {
    args = [].concat.apply({},args).slice(1);
    args.unshift(scope.toUpperCase()+":");
    self.winston[type](args.join(" "));    
  }
  
  this.info = function () {
    if (level<3) return;
    log('info', arguments);
  };

  this.warn = function () {
    if (level<2) return;
    log('warn', arguments);
  };  
  
  this.error = function () {
    if (level<1) return;
    log('error', arguments);
  };  
  
};

module.exports = function (scope) {
  return new Log(scope);
};