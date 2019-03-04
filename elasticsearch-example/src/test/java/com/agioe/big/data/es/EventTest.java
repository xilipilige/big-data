package com.agioe.big.data.es;

import com.agioe.big.data.es.model.Event;
import com.agioe.big.data.es.repository.EventRepository;
import com.google.gson.Gson;
import org.apache.commons.math3.random.RandomDataGenerator;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;

import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;

import org.springframework.data.elasticsearch.core.query.IndexQuery;

import org.springframework.test.context.junit4.SpringRunner;

import java.util.*;

/**
 * @author yshen
 * @since 19-3-4
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class EventTest {

    private static String INDEX_NAME = "monitor";
    private static String INDEX_TYPE = "event";

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    @Autowired
    private EventRepository eventRepository;


    @Test
    public void test() {
        while (true) {
            List<Event> eventList = constructModel();

            long start = System.currentTimeMillis();
            storeModel(eventList);
            long end = System.currentTimeMillis();
            System.out.println("耗时:" + (end - start));

            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    private static double roundDouble(double min, double max) {

        double v = new RandomDataGenerator().nextF(min, max);
        return v;
    }


    private static int roundInt(int min, int max) {

        int v = new RandomDataGenerator().nextInt(min, max);
        return v;
    }


    private List<Event> constructModel() {
        List<Event> list = new ArrayList<>();

        Date date = new Date();

        for (int i = 0; i < 100000; i++) {

            Event event = new Event();

            event.setId(UUID.randomUUID().toString());
            event.setDeviceCode("dev" + i);
            event.setEventCode("ec" + roundInt(1, 100));
            event.setEventValue(roundDouble(30.0, 40.0));
            event.setPropertyCode("pc" + roundInt(1, 100));
            event.setEventTime(date);
            event.setEventState(String.valueOf(roundInt(1,3)));
            event.setEventNature(String.valueOf(roundInt(1,3)));

            list.add(event);
        }
        return list;
    }

    private void storeModel(List<Event> eventList) {
        List<IndexQuery> queries = new ArrayList<>();
        Gson gson = new Gson();
        for (Event e : eventList) {
            IndexQuery indexQuery = new IndexQuery();
            indexQuery.setId(e.getId());
            indexQuery.setSource(gson.toJson(e));
            indexQuery.setIndexName(INDEX_NAME);
            indexQuery.setType(INDEX_TYPE);
            queries.add(indexQuery);
        }
        elasticsearchTemplate.bulkIndex(queries);
    }



    @Test
    public void count() {

        long count = eventRepository.count();
        System.out.println(count);

    }

    @Test
    public void groupByMultiFieldQuery() {

        Map map = new HashMap<>();
        //name和sum是别名
        TermsAggregationBuilder  pb = AggregationBuilders.terms("p").field("propertyCode");
        TermsAggregationBuilder  nb = AggregationBuilders.terms("n").field("eventNature");

        pb.subAggregation(nb);

        SearchRequestBuilder searchRequestBuilder =
                elasticsearchTemplate.getClient()
                        .prepareSearch("monitor")
                        .setTypes("event");

        searchRequestBuilder.addAggregation(pb);

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        Map<String, Aggregation> aggMap = searchResponse.getAggregations().asMap();


        StringTerms pTerms = (StringTerms) aggMap.get("p");

        Iterator<StringTerms.Bucket> propertyIterator = pTerms.getBuckets().iterator();

        while (propertyIterator.hasNext()){
            Terms.Bucket propertyBucket = propertyIterator.next();
            System.out.println(propertyBucket.getKey());
            System.out.println(propertyBucket.getDocCount());


            StringTerms nTerms = (StringTerms)propertyBucket.getAggregations().asMap().get("n");
            Iterator<StringTerms.Bucket> natureIterator= nTerms.getBuckets().iterator();
            while (natureIterator.hasNext()){
                Terms.Bucket natureBucket = natureIterator.next();
                System.out.println(natureBucket.getKey());
                System.out.println(natureBucket.getDocCount());
            }
        }






    }
}
