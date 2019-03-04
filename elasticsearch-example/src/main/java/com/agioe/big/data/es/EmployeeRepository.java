package com.agioe.big.data.es;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author yshen
 * @since 19-3-1
 */
@Component
public interface EmployeeRepository extends ElasticsearchRepository<Employee, String> {

    /**
     * 查询雇员信息
     *
     * @param id
     * @return
     */
    Employee queryEmployeeById(String id);

    /**
     * 字段过滤
     *
     * @param firstName
     * @return
     */
    Employee queryEmployeeByFirstName(String firstName);

    /**
     * 字段过滤+排序
     *
     * @param firstName
     * @return
     */
    List<Employee> queryEmployeeByFirstNameOrderByAgeAsc(String firstName);


    /**
     * 分页
     *
     * @param
     * @param pageable
     * @return
     */
    Page<Employee> getPageByFirstName(String firstName, Pageable pageable);


}

