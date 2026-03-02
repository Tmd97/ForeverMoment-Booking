package com.forvmom.MomentForeverBooking;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@SpringBootApplication
@EnableDiscoveryClient
public class MomentForeverBookingApplication {

	public static void main(String[] args) {
		SpringApplication.run(MomentForeverBookingApplication.class, args);
	}

}
