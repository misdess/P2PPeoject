/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * GVoD is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

package se.kth.swim.msg;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.javatuples.Pair;

import se.sics.p2ptoolbox.util.network.NatedAddress;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class Ping {
	
	//added
	private Pair<NatedAddress,Integer> alive;
	private Pair<NatedAddress,Integer> suspect;
	private Pair<NatedAddress,Integer> confirm;
	
	private int incarnation;
	
	public Ping(int inc){incarnation=inc;}
	
	public Ping(Pair<NatedAddress,Integer> alive, Pair<NatedAddress,Integer> suspect, Pair<NatedAddress,Integer> confirm, int inc) {
		super();
		this.alive = alive;
		this.suspect = suspect;
		this.confirm = confirm;
		incarnation=inc;
	}

	public Pair<NatedAddress,Integer> getAlive() {
		return alive;
	}

	public void setAlive(Pair<NatedAddress,Integer> alive) {
		this.alive = alive;
	}

	public Pair<NatedAddress,Integer> getSuspect() {
		return suspect;
	}

	public void setSuspect(Pair<NatedAddress,Integer> suspect) {
		this.suspect = suspect;
	}

	public Pair<NatedAddress,Integer> getConfirm() {
		return confirm;
	}

	public void setConfirm(Pair<NatedAddress,Integer> confirm) {
		this.confirm = confirm;
	}

	public int getIncarnation() {
		return incarnation;
	}

	public void setIncarnation(int incarnation) {
		this.incarnation = incarnation;
	}
	
	
}
