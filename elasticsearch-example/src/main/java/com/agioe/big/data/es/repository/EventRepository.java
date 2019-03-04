package com.agioe.big.data.es.repository;


import com.agioe.big.data.es.model.Event;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Component;

/**
 * @author yshen
 * @since 19-3-1
 */
@Component
public interface EventRepository extends ElasticsearchRepository<Event, String> {




}

