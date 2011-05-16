package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;


public class PutAction extends Action<Object> {
	
	private final Put put;

	public PutAction(Put put) {
		super();
		this.put = put;
	}

	public Put getPut() {
		return put;
	}

	@Override
	public Result perform() throws IOException {
		this.getTable().put(this.getPut());
		return null;
	}
	
}