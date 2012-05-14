#library('pipeline');
#import('dart:isolate');

typedef void Send(Object message);

typedef void Source(Send send); 
typedef void Processor(Object data, Send send); 
typedef void Sink(Object data); 

runSource(Source f) {
  port.receive((id, replyTo) {
    print("Source [$id] started");
    
    f((data) {
      replyTo.send({ "id": id, "data": data }, port.toSendPort());
    });
    
    port.close();
    print("Source [$id] terminated");
  });
}

runProcessor(Processor f) {
  port.receive((id, replyTo_) {
    print("Processor [$id] started");
    port.receive((message, replyTo) {
      switch (message['command']) {
        case 'process':
          f(message['data'], (data) {
            replyTo.send({ "id": id, "data": data }, port.toSendPort());
          });
          break;
        case 'terminate':
          port.close();
          print("Processor [$id] terminated");
          break;
      }
    });
  });
}

runSink(Sink f) {
  port.receive((id, replyTo_) {
    print("Sink [$id] started");
    port.receive((message, replyTo) {
      switch (message['command']) {
        case 'process':
          f(message['data']);
          replyTo.send({ "id": id, "data": "ok" }, port.toSendPort());
          break;
        case 'terminate':
          port.close();
          print("Sink [$id] terminated");
          break;
      }
    });
  });
}

runPipeline(templates) {
  var working = new Map<num, Set<SendPort>>();
  var available = new Map<num, List<SendPort>>();
  var idToIndex = new Map<Object, num>();
  
  idToIndex[templates[0].id] = 0;
  for (var i = 1; i < templates.length; i++) {
    working[i] = new Set();
    available[i] = [];
    idToIndex[templates[i].id] = i;
  }
  
  port.receive((message, replyTo) {
    var fromId = message["id"];
    var fromIndex = idToIndex[fromId];
    var data = message["data"];
    
    print("$fromIndex -> $message");
    
    if (fromIndex > 0) {
      working[fromIndex].remove(replyTo);
      available[fromIndex].add(replyTo);
    }
    
    var toIndex = fromIndex + 1;
    if (toIndex < templates.length) {
      if (available[toIndex].isEmpty()) {
        var instance = templates[toIndex].createInstance();
        available[toIndex].add(instance);
      }
      
      var instance = available[toIndex].removeLast();
      var outMsg = { 'command': 'process', 'data': data };
      print("$toIndex <- $outMsg");
      instance.send(outMsg, port.toSendPort());
      working[toIndex].add(instance);
    } else {
      var terminated = true;
      for (var i = 1; i < templates.length; i++) {
        if (!working[i].isEmpty()) {
          terminated = false;
          break;
        }
      }
      
      if (terminated) {
        for (var i = 1; i < templates.length; i++) {
          available[i].forEach((instance) {
            instance.send({ 'command': 'terminate' });
          });
        }
        
        port.close();
      }
    }
  });
  
  var source = templates[0].createInstance();
  source.send("go!", port.toSendPort());
}

interface Pipe {
  SendPort createInstance();
  get id();
}

class SpawnFunctionPipe implements Pipe {
  var count = 0;
  var id;
  var f;
  SpawnFunctionPipe(this.id, this.f);
  createInstance() {
    count++;
    print("Starting $count-th instance of [$id]");
    var sendPort = spawnFunction(f);
    sendPort.send(id, port.toSendPort());
    return sendPort;
  }
}
