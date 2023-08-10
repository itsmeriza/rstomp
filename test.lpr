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
  pub1, pub2, subsc1, subsc2: TRStomp;

{ TEventSub1 }

class procedure TEventSub1.RecvConnectedHandler(Frame: TRStompFrame);
begin
  WriteLn('');
  WriteLn('<<< Subsc 1 <<< ', frame.GetPacket());
  WriteLn();
end;

var
	ackId_1: string;

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
  PORT = 61613;

begin
 try
    WriteLn('*** Test publish/subscribe pattern *******************************');
    pub1:= TRStomp.Create('192.168.28.2', PORT);
    pub1.Login:= 'dev';
    pub1.Passcode:= 'dev123';
    pub1.Vhost:= '/';
    pub1.Connect();

    pub2:= TRStomp.Create('192.168.28.2', PORT);
    pub2.Login:= 'dev';
    pub2.Passcode:= 'dev123';
    pub2.Vhost:= '/';
    pub2.Connect();

    subsc1:= TRStomp.Create('192.168.28.2', PORT);
    subsc1.Login:= 'dev';
    subsc1.Passcode:= 'dev123';
    subsc1.Vhost:= '/';
    subsc1.OnRecvConnected:= @TEventSub1.RecvConnectedHandler;
    subsc1.OnRecvMessage:= @TEventSub1.RecvMessageHandler;
    subsc1.OnRecvReceipt:= @TEventSub1.RecvReceiptHandler;
    subsc1.OnRecvError:= @TEventSub1.RecvErrorHandler;
    subsc1.Connect();

    subsc2:= TRStomp.Create('192.168.28.2', PORT);
    subsc2.Login:= 'dev';
    subsc2.Passcode:= 'dev123';
    subsc2.Vhost:= '/';
    subsc2.OnRecvConnected:= @TEventSub2.RecvConnectedHandler;
    subsc2.OnRecvMessage:= @TEventSub2.RecvMessageHandler;
    subsc2.OnRecvReceipt:= @TEventSub2.RecvReceiptHandler;
    subsc2.OnRecvError:= @TEventSub2.RecvErrorHandler;
    subsc2.Connect();

    subsc1.Subscribe('sub-1', '/topic/sub1');
    subsc2.Subscribe('sub-2', '/topic/sub1');

    Sleep(2000);
    pub1.Send('/topic/sub1', 'HELLO');
    //pub1.Send('/topic/sub2', 'HELLO');


    Sleep(2000);
    WriteLn;
    WriteLn('*********** Queue Testing ***********');
    WriteLn;

    subsc1.Subscribe('sub-1-q', '/queue/sub1', TRStompAckType.atClient);

    Sleep(2000);
    pub1.Send('/queue/sub1', 'astalavista!');

    Sleep(2000);
    subsc1.Ack();

    //subsc1.Subscribe('sub-1', '/topic/pub');
    //subsc1.Subscribe('sub-2', '/topic/pub-2');
    //subsc2.Subscribe('sub-2', '/topic/pub');

    //Sleep(2000);
    //WriteLn('Publisher 1 send 1 message.');
    //pub1.Send('/topic/pub', 'this is first message.');
    //WriteLn('Publisher 1 send 1 message.');
    //pub1.Send('/topic/pub', 'this is second message.');
    //WriteLn('Publisher 1 send 1 message.');
    //pub1.Send('/topic/pub', 'this is third message.');
    //
    //
    //
    //WriteLn('Publisher 2 send 1 message.');
    //subsc1.Send('/topic/pub-2', 'this is first message sent by published 2.');

    Readln;
  except
    on E: Exception do
    begin
    	WriteLn('Connecting error with message: ', E.Message);
    end;
  end;
end.

