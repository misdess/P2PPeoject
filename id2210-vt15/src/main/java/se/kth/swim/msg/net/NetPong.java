package se.kth.swim.msg.net;

import se.kth.swim.msg.Pong;
import se.sics.kompics.network.Header;
import se.sics.p2ptoolbox.util.network.NatedAddress;

public class NetPong extends NetMsg<Pong> {

	

	public NetPong(NatedAddress src, NatedAddress dst, Pong pong) {
		super(src, dst, pong);
	}

	public NetPong(Header<NatedAddress> header, Pong content) {
		super(header, content);
	}

	public NetMsg copyMessage(Header<NatedAddress> newHeader) {
		return new NetPong(newHeader, getContent());
	}

}
