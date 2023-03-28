import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class SessionPoolExample {
    private static int iter = 10;
    private static FileChannel channel;

    private static SessionPool sessionPool;

    private static long pointNumber;

    private static String deviceId;

    public static void main(String[] args) throws FileNotFoundException {
        // 结果输出到文件
        String path = "iotdb_test.log";
        File logFile = new File(path);
        FileOutputStream fileOutputStream = new FileOutputStream(logFile, true);
        channel = fileOutputStream.getChannel();
        sessionPool =
                new SessionPool.Builder()
                        .host("10.246.128.15")
                        .port(6668)
                        .user("jianfei")
                        .password("123.com")
                        .maxSize(5)
                        .build();
        // 查询数据量
        queryPointsNumber();
//        queryDevicesName();
        // 具体查询测试
        queryTest1();
        queryTest2();
        queryTest3();
//        queryTest4();
        queryTest5();
        queryTest6();
        queryTest7();
        // 这些需要更新现在的 IoTDB 到最新版
//    queryTest8();
//    queryTest9();
//    queryTest10();
//    queryTest11();
//    queryTest12();
        sessionPool.close();
    }

    // 查询设备量

    private static void queryDevicesName(){
        SessionDataSetWrapper wrapper = null;
        //查询一辆车的路径
        try{
            String sql = "show devices root.text.** limit 1";
            wrapper = sessionPool.executeQueryStatement(sql);
            while (wrapper.hasNext()){
                RowRecord r = wrapper.next();
                deviceId = r.getFields().get(0).getStringValue();
            }
        }catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            // remember to close data set finally!
            sessionPool.closeResultSet(wrapper);
        }
    }

    // 查询一个车的总数据量
    private static void queryPointsNumber() {
        SessionDataSetWrapper wrapper = null;
        // 查询一辆车的路径
        try {
            String sql = "show devices root.text.** limit 1";
            wrapper = sessionPool.executeQueryStatement(sql);
            while (wrapper.hasNext()) {
                RowRecord r = wrapper.next();
                // 获得了一辆车的路径
                deviceId = r.getFields().get(0).getStringValue();
            }
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            // remember to close data set finally!
            sessionPool.closeResultSet(wrapper);
        }
        try {
            String sql = "select count(*) from " + deviceId + " group by level = 1";
            wrapper = sessionPool.executeQueryStatement(sql);
            // 这里正常只有一条数据
            while (wrapper.hasNext()) {
                RowRecord r = wrapper.next();
                pointNumber = r.getFields().get(0).getLongV();
            }
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            // remember to close data set finally!
            sessionPool.closeResultSet(wrapper);
        }
    }

    // 具体查询测试
    private static void queryTest1() {
        SessionDataSetWrapper wrapper = null;
        try {
            // 构造sql 语句
            String sql =
                    "select gw_nm from "
                            + deviceId
                            + " where time >= 2023-02-10T11:05:00 and time < 2023-02-23T12:05:00;";
            long totalCost = 0;
            for (int i = 0; i < iter; i++) {
                long startTime = System.currentTimeMillis();
                wrapper = sessionPool.executeQueryStatement(sql);
                // 遍历结果集
                while (wrapper.hasNext()) {
                    wrapper.next();
                }
                totalCost += System.currentTimeMillis() - startTime;
            }
            // 将查询耗时输出到文件中
            StringBuffer buf = new StringBuffer();
            buf.append("测试1\n");
            buf.append("描述: " + "查询某台车1小时时间段的数据\n");
            buf.append("数据量: " + pointNumber + "\n");
            buf.append("查询" + iter + "次平均耗时: " + totalCost / iter + "ms\n");
            buf.append("Sql: " + sql + "\n");
            buf.append("\n");
            buf.append("\n");
            channel.write(ByteBuffer.wrap(buf.toString().getBytes()));
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            // remember to close data set finally!
            sessionPool.closeResultSet(wrapper);
        }
    }

    private static void queryTest2() {
        SessionDataSetWrapper wrapper = null;
        try {
            // 构造sql 语句
            String sql =
                    "select count(gw_nm), max_value(CAST(gw_nm, 'type'='INT32')), min_value(CAST(gw_nm, 'type'='INT32')),sum(CAST(gw_nm, 'type'='INT32')),avg(CAST(gw_nm, 'type'='INT32')),min_time(gw_nm),max_time(gw_nm) from "
                            + deviceId
                            + " where time >= 2023-02-10T11:05:00 and time < 2023-02-23T12:05:00 and gw_nm in ('0', '1') limit 100;";
            long totalCost = 0;
            for (int i = 0; i < iter; i++) {
                long startTime = System.currentTimeMillis();
                wrapper = sessionPool.executeQueryStatement(sql);
                // 遍历结果集
                while (wrapper.hasNext()) {
                    wrapper.next();
                }
                totalCost += System.currentTimeMillis() - startTime;
            }
            // 将查询耗时输出到文件中
            StringBuffer buf = new StringBuffer();
            buf.append("测试2\n");
            buf.append("描述: " + "查询某台车1小时时间段的数据聚合信息\n");
            buf.append("数据量: " + pointNumber + "\n");
            buf.append("查询" + iter + "次平均耗时: " + totalCost / iter + "ms\n");
            buf.append("Sql: " + sql + "\n");
            buf.append("\n");
            buf.append("\n");
            channel.write(ByteBuffer.wrap(buf.toString().getBytes()));
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            // remember to close data set finally!
            sessionPool.closeResultSet(wrapper);
        }
    }

    private static void queryTest3() {
        SessionDataSetWrapper wrapper = null;
        try {
            // 构造sql 语句
            String sql =
                    "select gw_nm from "
                            + deviceId
                            + " where time >= 2023-02-10 and time < 2023-02-23 limit 10 offset 1";
            long totalCost = 0;
            for (int i = 0; i < iter; i++) {
                long startTime = System.currentTimeMillis();
                wrapper = sessionPool.executeQueryStatement(sql);
                // 遍历结果集
                while (wrapper.hasNext()) {
                    wrapper.next();
                }
                totalCost += System.currentTimeMillis() - startTime;
            }
            // 将查询耗时输出到文件中
            StringBuffer buf = new StringBuffer();
            buf.append("测试3\n");
            buf.append("描述: " + "分页查询某台车1天内常用指标\n");
            buf.append("数据量: " + pointNumber + "\n");
            buf.append("查询" + iter + "次平均耗时: " + totalCost / iter + "ms\n");
            buf.append("Sql: " + sql + "\n");
            buf.append("\n");
            buf.append("\n");
            channel.write(ByteBuffer.wrap(buf.toString().getBytes()));
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            // remember to close data set finally!
            sessionPool.closeResultSet(wrapper);
        }
    }

    private static void queryTest4() {
        SessionDataSetWrapper wrapper = null;
        try {
            // 构造sql 语句
            String sql =
                    "select veh_tot_distance from "
                            + deviceId
                            + " where time >= 2023-02-10 and time < 2023-02-23 and DIFF(CAST(veh_tot_distance, 'type'='DOUBLE')) != 0;";
            long totalCost = 0;
            for (int i = 0; i < iter; i++) {
                long startTime = System.currentTimeMillis();
                wrapper = sessionPool.executeQueryStatement(sql);
                // 遍历结果集
                while (wrapper.hasNext()) {
                    wrapper.next();
                }
                totalCost += System.currentTimeMillis() - startTime;
            }
            // 将查询耗时输出到文件中
            StringBuffer buf = new StringBuffer();
            buf.append("测试4\n");
            buf.append("描述: " + "查询单车1天内是否有里程跳变\n");
            buf.append("数据量: " + pointNumber + "\n");
            buf.append("查询" + iter + "次平均耗时: " + totalCost / iter + "ms\n");
            buf.append("Sql: " + sql + "\n");
            buf.append("\n");
            buf.append("\n");
            channel.write(ByteBuffer.wrap(buf.toString().getBytes()));
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            // remember to close data set finally!
            sessionPool.closeResultSet(wrapper);
        }
    }

    private static void queryTest5() {
        SessionDataSetWrapper wrapper = null;
        try {
            // 构造sql 语句
            String sql = "SELECT last gw_nm from " + deviceId;
            long totalCost = 0;
            for (int i = 0; i < iter; i++) {
                long startTime = System.currentTimeMillis();
                wrapper = sessionPool.executeQueryStatement(sql);
                // 遍历结果集
                while (wrapper.hasNext()) {
                    wrapper.next();
                }
                totalCost += System.currentTimeMillis() - startTime;
            }
            // 将查询耗时输出到文件中
            StringBuffer buf = new StringBuffer();
            buf.append("测试5\n");
            buf.append("描述: " + "查询单车的最近一条记录的时间\n");
            buf.append("数据量: " + pointNumber + "\n");
            buf.append("查询" + iter + "次平均耗时: " + totalCost / iter + "ms\n");
            buf.append("Sql: " + sql + "\n");
            buf.append("\n");
            buf.append("\n");
            channel.write(ByteBuffer.wrap(buf.toString().getBytes()));
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            // remember to close data set finally!
            sessionPool.closeResultSet(wrapper);
        }
    }

    private static void queryTest6() {
        SessionDataSetWrapper wrapper = null;
        try {
            // 构造sql 语句
            String sql = "SELECT last veh_tot_distance from " + deviceId;
            long totalCost = 0;
            for (int i = 0; i < iter; i++) {
                long startTime = System.currentTimeMillis();
                wrapper = sessionPool.executeQueryStatement(sql);
                // 遍历结果集
                while (wrapper.hasNext()) {
                    wrapper.next();
                }
                totalCost += System.currentTimeMillis() - startTime;
            }
            // 将查询耗时输出到文件中
            StringBuffer buf = new StringBuffer();
            buf.append("测试6\n");
            buf.append("描述: " + "查询单车的里程非空最新值\n");
            buf.append("数据量: " + pointNumber + "\n");
            buf.append("查询" + iter + "次平均耗时: " + totalCost / iter + "ms\n");
            buf.append("Sql: " + sql + "\n");
            buf.append("\n");
            buf.append("\n");
            channel.write(ByteBuffer.wrap(buf.toString().getBytes()));
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            // remember to close data set finally!
            sessionPool.closeResultSet(wrapper);
        }
    }

    private static void queryTest7() {
        SessionDataSetWrapper wrapper = null;
        try {
            // 构造sql 语句
            String sql =
                    "SELECT * from "
                            + deviceId
                            + " where time >= 2023-02-10 and time < 2023-02-23 and gw_nm='1';";
            long totalCost = 0;
            for (int i = 0; i < iter; i++) {
                long startTime = System.currentTimeMillis();
                wrapper = sessionPool.executeQueryStatement(sql);
                // 遍历结果集
                while (wrapper.hasNext()) {
                    wrapper.next();
                }
                totalCost += System.currentTimeMillis() - startTime;
            }
            // 将查询耗时输出到文件中
            StringBuffer buf = new StringBuffer();
            buf.append("测试7\n");
            buf.append("描述: " + "根据时间和其他指标值条件查询\n");
            buf.append("数据量: " + pointNumber + "\n");
            buf.append("查询" + iter + "次平均耗时: " + totalCost / iter + "ms\n");
            buf.append("Sql: " + sql + "\n");
            buf.append("\n");
            buf.append("\n");
            channel.write(ByteBuffer.wrap(buf.toString().getBytes()));
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            // remember to close data set finally!
            sessionPool.closeResultSet(wrapper);
        }
    }

    private static void queryTest8() {
        SessionDataSetWrapper wrapper = null;
        try {
            // 构造sql 语句
            String sql =
                    "select max_time(state_of_charge) - min_time(state_of_charge) from "
                            + deviceId
                            + " where time >= 2023-02-10 and time < 2023-02-23 group by series(state_of_charge='1', keep>= 2, ignoreNull=false)";
            long totalCost = 0;
            for (int i = 0; i < iter; i++) {
                long startTime = System.currentTimeMillis();
                wrapper = sessionPool.executeQueryStatement(sql);
                // 遍历结果集
                while (wrapper.hasNext()) {
                    wrapper.next();
                }
                totalCost += System.currentTimeMillis() - startTime;
            }
            // 将查询耗时输出到文件中
            StringBuffer buf = new StringBuffer();
            buf.append("测试8\n");
            buf.append("描述: " + "统计车辆在一段时间内充电的时长、次数\n");
            buf.append("数据量: " + pointNumber + "\n");
            buf.append("查询" + iter + "次平均耗时: " + totalCost / iter + "ms\n");
            buf.append("Sql: " + sql + "\n");
            buf.append("\n");
            buf.append("\n");
            channel.write(ByteBuffer.wrap(buf.toString().getBytes()));
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            // remember to close data set finally!
            sessionPool.closeResultSet(wrapper);
        }
    }

    private static void queryTest9() {
        SessionDataSetWrapper wrapper = null;
        try {
            // 构造sql 语句
            String sql =
                    "select first_value(bms_batt_soc), last_value(bms_batt_soc) from "
                            + deviceId
                            + " where time >= 2023-02-10 and time < 2023-02-23 group by series(state_of_charge='1', keep>= 2, ignoreNull=false)";
            long totalCost = 0;
            for (int i = 0; i < iter; i++) {
                long startTime = System.currentTimeMillis();
                wrapper = sessionPool.executeQueryStatement(sql);
                // 遍历结果集
                while (wrapper.hasNext()) {
                    wrapper.next();
                }
                totalCost += System.currentTimeMillis() - startTime;
            }
            // 将查询耗时输出到文件中
            StringBuffer buf = new StringBuffer();
            buf.append("测试9\n");
            buf.append("描述: " + "统计车辆在一段时间内开始充电时电量，结束充电时电量情况\n");
            buf.append("数据量: " + pointNumber + "\n");
            buf.append("查询" + iter + "次平均耗时: " + totalCost / iter + "ms\n");
            buf.append("Sql: " + sql + "\n");
            buf.append("\n");
            buf.append("\n");
            channel.write(ByteBuffer.wrap(buf.toString().getBytes()));
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            // remember to close data set finally!
            sessionPool.closeResultSet(wrapper);
        }
    }

    private static void queryTest10() {
        SessionDataSetWrapper wrapper = null;
        try {
            // 构造sql 语句
            String sql =
                    "select COUNT_IF(latitude = 0 and longitude = 0, 10, 'ignoreNull'='true') as abnormal_value_count from "
                            + deviceId
                            + " group by([2023-02-10, 2023-02-23), 1d)having COUNT_IF(latitude = 0 and longitude = 0, KEEP > 10, 'ignoreNull'='true') > 0";
            long totalCost = 0;
            for (int i = 0; i < iter; i++) {
                long startTime = System.currentTimeMillis();
                wrapper = sessionPool.executeQueryStatement(sql);
                // 遍历结果集
                while (wrapper.hasNext()) {
                    wrapper.next();
                }
                totalCost += System.currentTimeMillis() - startTime;
            }
            // 将查询耗时输出到文件中
            StringBuffer buf = new StringBuffer();
            buf.append("测试10\n");
            buf.append("描述: " + "统计时间段内某台车的定位异常天数, 连续为无效的次数大于10次（即GPS经纬度连续为0的次数大于等于10次），算一次定位异常\n");
            buf.append("数据量: " + pointNumber + "\n");
            buf.append("查询" + iter + "次平均耗时: " + totalCost / iter + "ms\n");
            buf.append("Sql: " + sql + "\n");
            buf.append("\n");
            buf.append("\n");
            channel.write(ByteBuffer.wrap(buf.toString().getBytes()));
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            // remember to close data set finally!
            sessionPool.closeResultSet(wrapper);
        }
    }

    private static void queryTest11() {
        SessionDataSetWrapper wrapper = null;
        try {
            // 构造sql 语句
            String sql =
                    "SELECT __endTime, first_value(gw_nm), last_value(gw_nm), max_time(gw_nm) - min_time(gw_nm) AS sleep_diff from "
                            + deviceId
                            + " where time >= 2023-02-10 and time < 2023-02-23 group by series(gw_nm NOT IN ('1','0'), KEEP >= 2, 'ignoreNull'='true')";
            long totalCost = 0;
            for (int i = 0; i < iter; i++) {
                long startTime = System.currentTimeMillis();
                wrapper = sessionPool.executeQueryStatement(sql);
                // 遍历结果集
                while (wrapper.hasNext()) {
                    wrapper.next();
                }
                totalCost += System.currentTimeMillis() - startTime;
            }
            // 将查询耗时输出到文件中
            StringBuffer buf = new StringBuffer();
            buf.append("测试11\n");
            buf.append("描述: " + "找出某车休眠时间的首末条数据，并计算出时间差，当gw_nm字段不是‘1’或者‘0’时，都认为车处于休眠状态\n");
            buf.append("数据量: " + pointNumber + "\n");
            buf.append("查询" + iter + "次平均耗时: " + totalCost / iter + "ms\n");
            buf.append("Sql: " + sql + "\n");
            buf.append("\n");
            buf.append("\n");
            channel.write(ByteBuffer.wrap(buf.toString().getBytes()));
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            // remember to close data set finally!
            sessionPool.closeResultSet(wrapper);
        }
    }

    private static void queryTest12() {
        SessionDataSetWrapper wrapper = null;
        try {
            // 构造sql 语句
            String sql =
                    "select __endTime, first_value(gw_nm) as first_gw_nm, last_value(gw_nm) as last_gw_nm from "
                            + deviceId
                            + " group by session(24000ms) having max_time(gw_nm) - min_time(gw_nm) >= 60000";
            long totalCost = 0;
            for (int i = 0; i < iter; i++) {
                long startTime = System.currentTimeMillis();
                wrapper = sessionPool.executeQueryStatement(sql);
                // 遍历结果集
                while (wrapper.hasNext()) {
                    wrapper.next();
                }
                totalCost += System.currentTimeMillis() - startTime;
            }
            // 将查询耗时输出到文件中
            StringBuffer buf = new StringBuffer();
            buf.append("测试12\n");
            buf.append("描述: " + "取出持续60s以上的连续信号的第一条和最后一条数据，信号连续的定义为，根据时间戳排序后，一台车的前后两条数据的时间差不超过24s\n");
            buf.append("数据量: " + pointNumber + "\n");
            buf.append("查询" + iter + "次平均耗时: " + totalCost / iter + "ms\n");
            buf.append("Sql: " + sql + "\n");
            buf.append("\n");
            buf.append("\n");
            channel.write(ByteBuffer.wrap(buf.toString().getBytes()));
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            // remember to close data set finally!
            sessionPool.closeResultSet(wrapper);
        }
    }
}
