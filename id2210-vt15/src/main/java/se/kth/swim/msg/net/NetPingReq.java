package se.kth.swim.msg.net;

import se.kth.swim.msg.PingReq;
import se.sics.kompics.network.Header;
import se.sics.p2ptoolbox.util.network.NatedAddress;

public class NetPingReq extends NetMsg<PingReq> {

	public NetPingReq(NatedAddress src, NatedAddress dest) {
		super(src, dest, new PingReq());
	}

	public NetPingReq(NatedAddress src, NatedAddress dest, PingReq pingReq) {
		super(src, dest, pingReq);
	}

	public NetPingReq(Header<NatedAddress> header, PingReq content) {
		super(header, content);
	}


	public NetMsg copyMessage(Header<NatedAddress> newHeader) {
		return null;
	}

}
