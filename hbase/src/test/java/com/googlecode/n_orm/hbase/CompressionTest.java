package com.googlecode.n_orm.hbase;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.googlecode.n_orm.Book;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.StoreSelector;
import com.googlecode.n_orm.hbase.Store;


public class CompressionTest {
	
	
	private static Store store;
	private static String testTable = "compressiontest";
	private static String defaultCompression;

	@BeforeClass
	public static void prepareStore() {
		HBaseLauncher.prepareHBase();
		store = HBaseLauncher.hbaseStore;
		defaultCompression = store.getCompression();
	}
	
	@Before
	@After
	public void deleteIfExists() throws IOException {
		if (store.getAdmin().tableExists(testTable)) {
			store.getAdmin().disableTable(testTable);
			store.getAdmin().deleteTable(testTable);
		}
		store.setCompression(defaultCompression);
	}
	
	@Test
	public void testNoCompressionNullDefined() throws IOException {
		store.setCompression(null);
		store.storeChanges(testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		assertEquals(Algorithm.NONE, propFamD.getCompression());
	}
	
	@Test
	public void testNoCompressionDefined() throws IOException {
		store.setCompression("none");
		store.storeChanges(testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		assertEquals(Algorithm.NONE, propFamD.getCompression());
	}
	
	@Test
	public void testGzCompressionDefined() throws IOException {
		assertTrue(org.apache.hadoop.hbase.util.CompressionTest.testCompression("gz"));
		store.setCompression("gz");
		store.storeChanges(testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		assertEquals(Algorithm.GZ, propFamD.getCompression());
	}
	
	@Test
	public void testLzoCompressionDefined() throws IOException {
		if (!org.apache.hadoop.hbase.util.CompressionTest.testCompression("lzo")) {
			System.err.println("TEST WARNING: no LZO compression enabled");
			return;
		}
		
		store.setCompression("lzo");
		store.storeChanges(testTable, "row", null, null, null);
		HColumnDescriptor propFamD = store.getAdmin().getTableDescriptor(Bytes.toBytes(testTable)).getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		assertEquals(Algorithm.LZO, propFamD.getCompression());
	}
}
