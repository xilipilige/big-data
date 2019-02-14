package com.agioe.big.data.redis.example.repository;

import com.agioe.big.data.redis.example.model.Event;
import org.springframework.data.repository.CrudRepository;

public interface EventRepository extends CrudRepository<Event,String> {
}
