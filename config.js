var config = {
    user: 'disp_server',
    password: 'disp_server',
    server: 'localhost\\SQLEXPRESS', // You can use 'localhost\\instance' to connect to named instance
    database: 'TD5R1',

    options: {
      encrypt: false // Use this if you're on Windows Azure
    }
  },
  amiConfig = {
    port: 5038,
    host: '',
    login: 'radminnode',
    password: '',
    encoding: 'ascii'
  },
  useAMIClient = false

module.exports.config = config
module.exports.amiConfig = amiConfig
module.exports.useAMIClient = useAMIClient
