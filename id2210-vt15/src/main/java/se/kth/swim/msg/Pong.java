package se.kth.swim.msg;

import java.util.Queue;
import java.util.Set;

import org.javatuples.Pair;

import se.sics.p2ptoolbox.util.network.NatedAddress;

public class Pong {	
	//
	private Queue<Pair<NatedAddress,Integer>> New;	
	private Queue<Pair<NatedAddress,Integer>> Failed;	
	private Queue<Pair<NatedAddress,Integer>> Suspected;
	
	private NatedAddress susp;
	private int incar;

	public Pong(int in){this.incar=in;}


	public Pong(Queue<Pair<NatedAddress,Integer>> new1, Queue<Pair<NatedAddress,Integer>> failed, Queue<Pair<NatedAddress,Integer>> suspected, int in) {
		super();
		New = new1;
		Failed = failed;
		Suspected = suspected;
		incar=in;
	}
	
	public Pong(NatedAddress suspected, int in) {
		super();
		susp = suspected;
		incar=in;
	}
	
	
	public Queue<Pair<NatedAddress,Integer>> getNew() {
		return New;
	}

	public Queue<Pair<NatedAddress,Integer>> getFailed() {
		return Failed;
	}

	public Queue<Pair<NatedAddress,Integer>> getSuspected() {
		return Suspected;
	}
	
	public NatedAddress getSusp(){return this.susp;}


	public int getIncar() {
		return incar;
	}

	

}
