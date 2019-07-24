
package com.coco.controller;

import com.coco.service.PersonService;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.text.ParseException;

/**
 * @Description: TODO
 * @author zhangxiaoxun
 * @date 2019/7/21 18:27
 * @Version: V1.0
 *
 **/
@Controller
public class PersonController {
    @Autowired
    private PersonService personService;

    @PutMapping("person/insert")
    public @ResponseBody  IndexResponse addPerson(){
        return personService.addPerson();
    }

    @PutMapping("person/bulkAdd")
    public @ResponseBody   BulkResponse bulkAdd(){
        return personService.bulkAdd();
    }

    @PutMapping("person/deletePersonById")
    public @ResponseBody   DeleteResponse deletePersonById() throws IOException {
        return personService.deletePersonById();
    }

    @GetMapping("person/search")
    public @ResponseBody    SearchResponse search() throws IOException, ParseException {
        return personService.search();
    }

    @PutMapping("person/update")
    public @ResponseBody  UpdateResponse update()  {
        return personService.update();
    }





}
