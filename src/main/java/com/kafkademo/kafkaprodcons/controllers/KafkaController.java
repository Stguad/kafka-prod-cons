package com.kafkademo.kafkaprodcons.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;
import com.kafkademo.kafkaprodcons.models.MoreSimpleModel;
import com.kafkademo.kafkaprodcons.models.SimpleModel;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {

	private KafkaTemplate<String, String> kafkaTemplate;	
	private Gson jsonConverter;
	
	@Autowired
	public KafkaController(KafkaTemplate<String, String> kafkaTemplate, Gson jsonConverter) {
		this.kafkaTemplate = kafkaTemplate;
		this.jsonConverter = jsonConverter;
	}

//	@PostMapping
//	public void post(@RequestBody SimpleModel simpleModel) {
//	
//		kafkaTemplate.send("Test", simpleModel);
//	}
//	
//	@KafkaListener(topics = "Test")
//	public void getFromKafka(SimpleModel simpleModel) {
//		
//		System.out.println(simpleModel.toString());
//	}
	
	@PostMapping
	public void post(@RequestBody SimpleModel simpleModel) {
	
		kafkaTemplate.send("Test", jsonConverter.toJson(simpleModel));
	}
	
	@KafkaListener(topics = "Test")
	public void getFromKafka(String simpleModel) {
		
		System.out.println(simpleModel.toString());
		
		SimpleModel sm = jsonConverter.fromJson(simpleModel, SimpleModel.class);
		
		System.out.println(sm.toString());
	}
	
	@PostMapping("/v2")
	public void post(@RequestBody MoreSimpleModel moreSimpleModel) {
	
		kafkaTemplate.send("Test2", jsonConverter.toJson(moreSimpleModel));
	}
	
	@KafkaListener(topics = "Test2")
	public void getFromKafka2(String moreSimpleModel) {
		
		System.out.println(moreSimpleModel.toString());
		
		MoreSimpleModel msm = jsonConverter.fromJson(moreSimpleModel, MoreSimpleModel.class);
		
		System.out.println(msm.toString());
	}
}
