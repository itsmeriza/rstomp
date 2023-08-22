program test;

uses Classes, SysUtils, rstomp, IdSSLOpenSSL;

type

  { TEvent }

 TEvent = class
    class procedure RecvConnectedHandler(Frame: TRStompFrame);
    class procedure RecvMessageHandler(Frame: TRStompFrame);
    class procedure RecvReceiptHandler(Frame: TRStompFrame);
    class procedure RecvErrorHandler(Frame: TRStompFrame);
  end;

 { TEventPub }

 { TEventSub1 }

 TEventSub1 = class
    class procedure RecvConnectedHandler(Frame: TRStompFrame);
    class procedure RecvMessageHandler(Frame: TRStompFrame);
    class procedure RecvReceiptHandler(Frame: TRStompFrame);
    class procedure RecvErrorHandler(Frame: TRStompFrame);
 end;

  { TEventSub2 }

  TEventSub2 = class
    class procedure RecvConnectedHandler(Frame: TRStompFrame);
    class procedure RecvMessageHandler(Frame: TRStompFrame);
    class procedure RecvReceiptHandler(Frame: TRStompFrame);
    class procedure RecvErrorHandler(Frame: TRStompFrame);
 end;

var
  i: Integer;

{ TEventSub1 }

class procedure TEventSub1.RecvConnectedHandler(Frame: TRStompFrame);
begin
  WriteLn('');
  WriteLn('<<< Subsc 1 <<< ', frame.GetPacket());
  WriteLn();
end;

class procedure TEventSub1.RecvMessageHandler(Frame: TRStompFrame);
begin
  WriteLn('Ack id: ', Frame.GetAckId());
	TEventSub1.RecvConnectedHandler(Frame);
end;

class procedure TEventSub1.RecvReceiptHandler(Frame: TRStompFrame);
begin
  TEventSub1.RecvConnectedHandler(Frame);
end;

class procedure TEventSub1.RecvErrorHandler(Frame: TRStompFrame);
begin
  TEventSub1.RecvConnectedHandler(Frame);
end;

{ TEventSub2 }

class procedure TEventSub2.RecvConnectedHandler(Frame: TRStompFrame);
begin
  WriteLn('');
  WriteLn('<<< Subsc 2 <<< ', frame.GetPacket());
  WriteLn();
end;

class procedure TEventSub2.RecvMessageHandler(Frame: TRStompFrame);
begin
	TEventSub2.RecvConnectedHandler(Frame);
end;

class procedure TEventSub2.RecvReceiptHandler(Frame: TRStompFrame);
begin
  TEventSub2.RecvConnectedHandler(Frame);
end;

class procedure TEventSub2.RecvErrorHandler(Frame: TRStompFrame);
begin
	TEventSub2.RecvConnectedHandler(Frame);
end;

{ TEvent }

class procedure TEvent.RecvConnectedHandler(Frame: TRStompFrame);
begin
  WriteLn();
  WriteLn('<<< ', frame.GetPacket());
  WriteLn();
end;

class procedure TEvent.RecvMessageHandler(Frame: TRStompFrame);
begin
	RecvConnectedHandler(Frame);
end;

class procedure TEvent.RecvReceiptHandler(Frame: TRStompFrame);
begin
  RecvConnectedHandler(Frame);
end;

class procedure TEvent.RecvErrorHandler(Frame: TRStompFrame);
begin
  RecvConnectedHandler(Frame);
end;

const
  BROKER_ADDRESS = '192.168.28.2';
  BROKER_PORT = 61613;
  BROKER_VHOST = '/';
  BROKER_USER = 'dev';
  BROKER_PASSWD = 'dev123';

  PATH_TOPIC_3 = '/topic/node3';
  PATH_QUEUE_2 = '/queue/node2';
  PATH_QUEUE_3 = '/queue/node3';

var
	isPublishSubscribePatternTest: Boolean;
  isWaitResponseTesting: Boolean;
  node: TRStomp;

procedure DoPublishSubscribePatternTest();
var
  node_1, node_2, node_3: TRStomp;
begin
  WriteLn();
  WriteLn('*********** Publish/Subscrive Pattern Test Begin ************');
  WriteLn();

  node_1:= TRStomp.Create(BROKER_ADDRESS, BROKER_PORT);
  node_1.Login:= BROKER_USER;
  node_1.Passcode:= BROKER_PASSWD;
  node_1.Vhost:= BROKER_VHOST;
  node_1.OnRecvMessage:= @TEvent.RecvMessageHandler;
  node_1.Connect();

  node_2:= TRStomp.Create(BROKER_ADDRESS, BROKER_PORT);
  node_2.Login:= BROKER_USER;
  node_2.Passcode:= BROKER_PASSWD;
  node_2.Vhost:= BROKER_VHOST;
  node_2.OnRecvMessage:= @TEvent.RecvMessageHandler;
  node_2.Connect();

  node_3:= TRStomp.Create(BROKER_ADDRESS, BROKER_PORT);
  node_3.Login:= BROKER_USER;
  node_3.Passcode:= BROKER_PASSWD;
  node_3.Vhost:= BROKER_VHOST;
  node_3.OnRecvMessage:= @TEvent.RecvMessageHandler;
  node_3.Connect();

  // node_1, node_2, node_3 subcribe node_3
  node_1.Subscribe('sub-node-1', PATH_TOPIC_3);
  node_2.Subscribe('sub-node-2', PATH_TOPIC_3);
  node_2.Subscribe('sub-node-3', PATH_TOPIC_3);

  // node_3 broadcats a message through the PATH_TOPIC_3
  Sleep(2000);
  node_3.Send(PATH_TOPIC_3, 'This message sent by node_3.');
end;

procedure DoWaitResponseTesting();
var
  response: String;
  data: RDataSendWait;
  node: TRStomp;
begin
  WriteLn();
  WriteLn('*********** Wait Response Test Begin ************');
  WriteLn();

  node:= TRStomp.Create(BROKER_ADDRESS, BROKER_PORT);
  node.Login:= BROKER_USER;
  node.Passcode:= BROKER_PASSWD;
  node.Vhost:= BROKER_VHOST;
  node.Connect();

  data.Destination:= PATH_QUEUE_3;
  data.Listener:= PATH_QUEUE_3;
  data.Body:= 'Message ' + IntToStr(i+1);
  data.SubscriptionId:= TRStompUtils.CreateGUID();

  response:= node.SendWait(data);
  WriteLn('Response:');
  WriteLn(response);
end;

begin
  isPublishSubscribePatternTest:= False;
  isWaitResponseTesting:= True;

 	try
    if isPublishSubscribePatternTest then
    	DoPublishSubscribePatternTest();

    if isWaitResponseTesting then
    begin
      i:= 0;
      while i < 100 do
      begin
      	DoWaitResponseTesting();
        Inc(i);
      end;
    end;

    Readln;
  except
    on E: Exception do
    begin
    	WriteLn('Connecting error with message: ', E.Message);
    end;
  end;
end.

