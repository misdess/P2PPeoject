package se.kth.swim.msg;

import java.util.Set;

import org.javatuples.Pair;

import se.sics.p2ptoolbox.util.network.NatedAddress;

public class PingReq {
	
	private Pair<NatedAddress,Integer> suspected;
	
	public PingReq(){}

	public PingReq(Pair<NatedAddress,Integer> suspected) {
		super();
		this.suspected = suspected;
	}

	public Pair<NatedAddress,Integer> getSuspected() {
		return suspected;
	}
	
}
