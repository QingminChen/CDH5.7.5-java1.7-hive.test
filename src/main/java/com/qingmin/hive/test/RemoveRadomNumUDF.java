package com.qingmin.hive.test;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class RemoveRadomNumUDF extends UDF{

	/**
	 * 对于这个方法的参数和返回值都使用writable的子类
	 * */
	public Text evaluate(Text str){
		String result = str.toString().substring(str.find("_")+1);
		return new Text(result);
		
	}
	
	public static void main(String[] args){
		RemoveRadomNumUDF removeRadomNumUDF = new RemoveRadomNumUDF();
		System.out.print(removeRadomNumUDF.evaluate(new Text("8_abc")));

	}
}
