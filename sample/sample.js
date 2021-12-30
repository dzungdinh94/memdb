const net = require("net");
net.createServer().listen(52117, "127.0.0.1");

const client = net.createConnection({ port: 52117,  }, () => {
  // 'connect' listener.
  console.log("connected to server!");
});
client.on("error", function (e) {
  console.log(e);
  console.log(e.message);
});
client.on("data", (data) => {
  console.log(data.toString());
  client.end();
});
client.on("end", () => {
  console.log("disconnected from server");
});

client.on("close", () => {
  console.log("close from server");
});
