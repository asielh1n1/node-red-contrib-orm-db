module.exports = function(RED) {
    function Connection(config) {
        RED.nodes.createNode(this, config);
        this.name = config.name;
        this.driver = config.driver;
        this.host = config.host;
        this.port = config.port;
        this.username = this.credentials && this.credentials.username ? this.credentials.username : '' ;        
        this.password = this.credentials && this.credentials.password ? this.credentials.password : '';
        this.database = config.database;
        this.dialectOptions = config.dialectOptions
        this.dialectOptionsType = config.dialectOptionsType
        this.logging = config.logging
    }
    RED.nodes.registerType("orm-db-connection", Connection, {
        credentials: {
            username: { type: "text" },
            password: { type: "password" }
        }
    });
}
