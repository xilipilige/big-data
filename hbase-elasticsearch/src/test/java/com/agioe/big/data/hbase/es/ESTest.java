package com.agioe.big.data.hbase.es;

import com.agioe.big.data.hbase.es.es.Employee;
import com.agioe.big.data.hbase.es.es.EmployeeRepository;
import com.google.gson.Gson;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.ResultsExtractor;
import org.springframework.data.elasticsearch.core.query.*;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.*;


/**
 * @author yshen
 * @since 19-3-1
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class ESTest {

    @Autowired
    private EmployeeRepository employeeRepository;

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;



    @Test
    public void refresh(){
        elasticsearchTemplate.refresh("company");
    }

    /**
     * 添加
     *
     * @return
     */
    @Test
    public void add() {
        Employee employee = new Employee();
        employee.setId(UUID.randomUUID().toString());
        employee.setFirstName("jack");
        employee.setLastName("ma");
        employee.setAge(21);
        employee.setAbout("i am in peking");
        employeeRepository.save(employee);
        System.err.println("add a obj");
    }

    /**
     * 批量新增
     */
    @Test
    public void batchAdd() {
        List<Employee> employeeList = new ArrayList<>();

        Employee employee1 = new Employee();
        employee1.setId(UUID.randomUUID().toString());
        employee1.setFirstName("hh");
        employee1.setLastName("yy");
        employee1.setAge(21);
        employee1.setAbout("i am in peking");

        employeeList.add(employee1);


        Employee employee2 = new Employee();
        employee2.setId(UUID.randomUUID().toString());
        employee2.setFirstName("gg");
        employee2.setLastName("ff");
        employee2.setAge(21);
        employee2.setAbout("i am in peking");

        employeeList.add(employee2);


        List<IndexQuery> queries = new ArrayList<>();
        Gson gson = new Gson();
        for (Employee e : employeeList) {
            IndexQuery indexQuery = new IndexQuery();
            indexQuery.setId(e.getId());
            indexQuery.setSource(gson.toJson(e));
            indexQuery.setIndexName("company");
            indexQuery.setType("employee");
            queries.add(indexQuery);
        }

        elasticsearchTemplate.bulkIndex(queries);
        //刷新操作 否则不会实时建立索引
        elasticsearchTemplate.refresh("company");
    }

    /**
     * 删除
     *
     * @return
     */
    @Test
    public void delete() {
        Employee employee = employeeRepository.queryEmployeeById("1");
        employeeRepository.delete(employee);
    }

    /**
     * 更新
     *
     * @return
     */
    @Test
    public void update() {
        Employee employee = employeeRepository.queryEmployeeById("1");
        employee.setFirstName("哈哈");
        employeeRepository.save(employee);
        System.err.println("update a obj");
    }

    /**
     * 根据条件更新
     *
     * @see :https://blog.csdn.net/shihlei/article/details/84827595
     */
    @Test
    public void updateByQuery() {
        String index = "company";

        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.must(QueryBuilders.termQuery("firstName", "wu"));

        UpdateByQueryRequestBuilder updateByQuery = UpdateByQueryAction.INSTANCE.newRequestBuilder(elasticsearchTemplate.getClient())
                .filter(queryBuilder)
                .source(index)
                .script(new Script("ctx._source.lastName='cc'"));

        BulkByScrollResponse response = updateByQuery.get();
//        elasticsearchTemplate.refresh("company");


    }

    /**
     * 查询
     *
     * @return
     */
    @Test
    public void query() {
        Employee employee = employeeRepository.queryEmployeeById("1");
        System.err.println(new Gson().toJson(employee));
    }


    /**
     * 条件查询
     *
     * @return
     */
    @Test
    public void queryByField() {
        List<Employee> employee = employeeRepository.queryEmployeeByFirstNameOrderByAgeAsc("xuxu");
        System.err.println(new Gson().toJson(employee));
    }


    /**
     * 分页查询
     *
     * @return
     * @see: https://blog.csdn.net/larger5/article/details/79777319
     */
    @Test
    public void queryByPage() {
        Pageable pageable = PageRequest.of(0, 3);
        Page<Employee> employees = employeeRepository.getPageByFirstName("xuxu", pageable);
        System.err.println(new Gson().toJson(employees));
    }


    /**
     * 查询所有
     *
     * @return
     */
    @Test
    public void queryAll() {
        Iterable<Employee> iterable = employeeRepository.findAll();

        iterable.forEach(s -> {
            System.err.println(new Gson().toJson(s));

        });

    }

    /**
     * 查询个数
     *
     * @return
     */
    @Test
    public void queryCount() {
        long count = employeeRepository.count();
        System.out.println(count);
    }

    /**
     * 多条件组合查询
     *
     * @see: https://blog.csdn.net/u014016928/article/details/41723485
     */
    @Test
    public void queryCriteria() {
        CriteriaQuery criteriaQuery = new CriteriaQuery(
                new Criteria()
                        .and(new Criteria("firstName").is("xuxu"))
                        .and(new Criteria("lastName").is("zh"))
                        .and(new Criteria("about").is("i am in peking"))
        );
        List<Employee> employee = elasticsearchTemplate.queryForList(criteriaQuery, Employee.class);
        System.err.println(new Gson().toJson(employee));
    }


    @Test
    /**
     * In 查询
     * @see :http://www.cnblogs.com/wenbronk/p/6432990.html
     */
    public void queryIn() {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("lastName", "shen"))
                .should(QueryBuilders.termQuery("lastName", "sun"));

        SearchRequestBuilder searchRequestBuilder =
                elasticsearchTemplate.getClient()
                        .prepareSearch("company")
                        .setTypes("employee")
                        .setFetchSource("age", null);

        SearchResponse searchResponse = searchRequestBuilder.setQuery(boolQueryBuilder).execute().actionGet();


        for (SearchHit hit : searchResponse.getHits()) {
            Iterator<Map.Entry<String, Object>> iterator = hit.getSource().entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> next = iterator.next();
                System.out.println(next.getKey() + ": " + next.getValue());
                if (searchResponse.getHits().hits().length == 0) {
                    break;
                }
            }
        }
    }

    /**
     * 分组查询
     *
     * @see :https://blog.csdn.net/u011403655/article/details/71107415
     * https://www.cnblogs.com/ouyanxia/p/8288749.html
     */
    @Test
    public void groupQuery() {

        Map map = new HashMap<>();
        //name和sum是别名
        TermsAggregationBuilder tb = AggregationBuilders.terms("name").field("firstName");
        SumAggregationBuilder sb = AggregationBuilders.sum("sum").field("age");

        tb.subAggregation(sb);
        BoolQueryBuilder bqb = QueryBuilders.boolQuery();
        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(bqb).withIndices("company").withTypes("employee")
                .addAggregation(tb)
                .build();
        Aggregations aggregations = elasticsearchTemplate.query(searchQuery, new ResultsExtractor<Aggregations>() {
            @Override
            public Aggregations extract(SearchResponse response) {
                return response.getAggregations();
            }
        });
        Terms term = aggregations.get("name");
        if (term.getBuckets().size() > 0) {
            for (Terms.Bucket bk : term.getBuckets()) {
                long count = bk.getDocCount();
                Map subaggmap = bk.getAggregations().asMap();
                double amount = ((InternalSum) subaggmap.get("sum")).getValue();
                System.out.println(bk.getKey());
                System.out.println(count);
                System.out.println(amount);
                map.put("count", count);
                map.put("amount", amount);
            }
        }


    }

}
