package com.example.ocrclient.util;

import com.example.ocrclient.ai.SingleResult;
import com.example.ocrclient.ai.SingleResultSeq;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * 工具类，用于处理结果排序
 */
public class ResultSortUtil {
    
    /**
     * 对SingleResult序列按照task_id进行排序
     * @param results SingleResult序列
     * @return 按照task_id递增排序的列表
     */
    public static List<SingleResult> sortResultsByTaskId(SingleResultSeq results) {
        if (results == null || results.length() <= 0) {
            return new ArrayList<>();
        }
        
        // 将结果转换为List
        List<SingleResult> resultList = new ArrayList<>();
        for (int i = 0; i < results.length(); i++) {
            resultList.add(results.get_at(i));
        }
        
        // 按照task_id排序
        Collections.sort(resultList, new Comparator<SingleResult>() {
            @Override
            public int compare(SingleResult r1, SingleResult r2) {
                try {
                    int taskId1 = Integer.parseInt(r1.task.task_id);
                    int taskId2 = Integer.parseInt(r2.task.task_id);
                    return Integer.compare(taskId1, taskId2);
                } catch (NumberFormatException e) {
                    // 如果无法解析为整数，则按字符串比较
                    return r1.task.task_id.compareTo(r2.task.task_id);
                }
            }
        });
        
        return resultList;
    }
}