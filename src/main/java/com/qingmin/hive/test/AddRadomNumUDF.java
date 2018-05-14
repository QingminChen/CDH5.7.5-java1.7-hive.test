package com.qingmin.hive.test;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class AddRadomNumUDF extends UDF{

	/**
	 * 对于这个方法的参数和返回值都使用writable的子类
	 * */
	public Text evaluate(Text str){
		int num=(int)(Math.random() * 11);//产生10以内的随机数
		return new Text(num+"_"+str);
		
	}
	
	public static void main(String[] args){
		AddRadomNumUDF addRadomNumUDF = new AddRadomNumUDF();
		System.out.print(addRadomNumUDF.evaluate(new Text("abc")));

	}
}
