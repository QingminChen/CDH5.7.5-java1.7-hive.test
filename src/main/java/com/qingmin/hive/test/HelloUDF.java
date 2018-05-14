package com.qingmin.hive.test;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class HelloUDF extends UDF{
	
	/**
	 * 对于这个方法的参数和返回值都使用writable的子类
	 * */
	public Text evaluate(Text name){
		return new Text("Hello "+name);
		
	}
	
	public Text evaluate(Text name,IntWritable age){
		return new Text("Hello "+name+",your age is "+age+".");
		
	}
	
	public static void main(String[] args){
		HelloUDF helloUDF = new HelloUDF();
		System.out.print(helloUDF.evaluate(new Text("Qingmin"),new IntWritable(30)));
		
		//new FunctionInfo 
	}

}
