package com.agioe.big.data.redis.example.repository;

import com.agioe.big.data.redis.example.model.Person;
import org.springframework.data.repository.CrudRepository;

public interface PersonRepository extends CrudRepository<Person, String> {
}
