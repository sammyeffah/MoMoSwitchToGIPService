package com.etz.gh;

import com.etz.gh.model.SwitchGIPRequest;
import com.etz.gh.model.SwitchGIPResponse;
import io.agroal.api.AgroalDataSource;
import org.jboss.logmanager.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Path("/usegip")
public class Resource {

    static Logger logger = Logger.getLogger("SWITCH MOMO TO GIP SERVICE");
    @Inject
    AgroalDataSource dataSource;

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String getTime() {
        String toReturn = null;
        try (
                Connection con = dataSource.getConnection();
                PreparedStatement ps = con.prepareStatement("SELECT CURRENT_TIMESTAMP")) {
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                toReturn = "Current time: " + rs.getTimestamp(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return toReturn;
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/switch")
    public String switchUseGIP(SwitchGIPRequest switchGIPRequest) {

        SwitchGIPResponse switchGIPResponse = new SwitchGIPResponse();
        Connection con = null;
        PreparedStatement ps = null;
        try {
            if (!switchGIPRequest.getNetwork().isEmpty()) {
                long start = System.currentTimeMillis();
                con = dataSource.getConnection();

                int rs;
                int finalsRes = -1;
                switch (switchGIPRequest.getNetwork()) {
                    case "686" -> {
                        logger.info("SWITCH STATUS:: " + switchGIPRequest.getSwitchToGIP());
                        if (!switchGIPRequest.getSwitchToGIP().isEmpty()) {
                            logger.info("I'M IN GIP SWITCH NOT BEING EMPTY");
                            if (switchGIPRequest.getBankCode().isEmpty() && !switchGIPRequest.getExcludedBank().isEmpty()) {
                                logger.info("I GOT HERE EXCLUDED");
                                finalsRes = includeEmptyExcludeNotEmpty(switchGIPRequest);
                            } else if (!switchGIPRequest.getBankCode().isEmpty() && !switchGIPRequest.getExcludedBank().isEmpty()) {
                                logger.info("I GOT HERE THE DOUBLE!!!");
                                finalsRes = includeNotEmptyExcludeNotEmpty(switchGIPRequest);
                            } else if (!switchGIPRequest.getBankCode().isEmpty() && switchGIPRequest.getExcludedBank().isEmpty()) {
                                logger.info("I GOT HERE INCLUDED");
                                finalsRes = includeNotEmptyExcludeEmpty(switchGIPRequest);
                            } else if (switchGIPRequest.getExcludedBank().isEmpty() && switchGIPRequest.getBankCode().isEmpty()) {
                                finalsRes = updateOnlySwitch(switchGIPRequest);
                            }
                        } else {
                            logger.info("I'M IN GIP SWITCH BEING EMPTY");
                            if (switchGIPRequest.getBankCode().isEmpty() && !switchGIPRequest.getExcludedBank().isEmpty()) {
                                logger.info("I GOT HERE EXCLUDED");
                                finalsRes = includeEmptyExcludeNotEmpty(switchGIPRequest);
                            } else if (!switchGIPRequest.getBankCode().isEmpty() && !switchGIPRequest.getExcludedBank().isEmpty()) {
                                logger.info("I GOT HERE THE DOUBLE!!!");
                                finalsRes = includeNotEmptyExcludeNotEmpty(switchGIPRequest);
                            } else if (!switchGIPRequest.getBankCode().isEmpty() && switchGIPRequest.getExcludedBank().isEmpty()) {
                                logger.info("I GOT HERE INCLUDED");
                                finalsRes = includeNotEmptyExcludeEmpty(switchGIPRequest);
                            }
                        }

                        logger.info("FINAL RESP::: " + finalsRes);
                        if (finalsRes == 1) {
                            switchGIPResponse.setError("00");
                            switchGIPResponse.setMessage("Success");
                        } else if (finalsRes == 2) {
                            switchGIPResponse.setError("06");
                            switchGIPResponse.setMessage("Already included or excluded, or must be taken from included or excluded data!!!");
                        } else {
                            switchGIPResponse.setError("06");
                            switchGIPResponse.setMessage("Update failed!!");
                        }

                    }
                    case "863" -> {

                        if (switchGIPRequest.getBankCode().isEmpty() && !switchGIPRequest.getExcludedBank().isEmpty()) {
                            logger.info("I GOT HERE EXCLUDED");
                            finalsRes = includeEmptyExcludeNotEmptyVoda(switchGIPRequest);
                        } else if (!switchGIPRequest.getBankCode().isEmpty() && !switchGIPRequest.getExcludedBank().isEmpty()) {
                            logger.info("I GOT HERE THE DOUBLE!!!");
                            finalsRes = includeNotEmptyExcludeNotEmptyVoda(switchGIPRequest);
                        } else if (!switchGIPRequest.getBankCode().isEmpty() && switchGIPRequest.getExcludedBank().isEmpty()) {
                            logger.info("I GOT HERE INCLUDED");
                            finalsRes = includeNotEmptyExcludeEmptyVoda(switchGIPRequest);
                        } else if (switchGIPRequest.getExcludedBank().isEmpty() && switchGIPRequest.getBankCode().isEmpty()) {
                            finalsRes = updateOnlySwitch(switchGIPRequest);
                        }

                        logger.info("FINAL RESP::: " + finalsRes);
                        if (finalsRes == 1) {
                            switchGIPResponse.setError("00");
                            switchGIPResponse.setMessage("Success");
                        } else if (finalsRes == 2) {
                            switchGIPResponse.setError("06");
                            switchGIPResponse.setMessage("Already included or excluded, or must be taken from included or excluded data!!!");
                        } else {
                            switchGIPResponse.setError("06");
                            switchGIPResponse.setMessage("Update failed!!");
                        }

                    }
                    case "844" -> {

                        if (switchGIPRequest.getBankCode().isEmpty() && !switchGIPRequest.getExcludedBank().isEmpty()) {
                            logger.info("I GOT HERE EXCLUDED");
                            finalsRes = includeEmptyExcludeNotEmptyAirtelTigo(switchGIPRequest);
                        } else if (!switchGIPRequest.getBankCode().isEmpty() && !switchGIPRequest.getExcludedBank().isEmpty()) {
                            logger.info("I GOT HERE THE DOUBLE!!!");
                            finalsRes = includeNotEmptyExcludeNotEmptyAirtelTigo(switchGIPRequest);
                        } else if (!switchGIPRequest.getBankCode().isEmpty() && switchGIPRequest.getExcludedBank().isEmpty()) {
                            logger.info("I GOT HERE INCLUDED");
                            finalsRes = includeNotEmptyExcludeEmptyAirtelTigo(switchGIPRequest);
                        } else if (switchGIPRequest.getExcludedBank().isEmpty() && switchGIPRequest.getBankCode().isEmpty()) {
                            finalsRes = updateOnlySwitch(switchGIPRequest);
                        }

                        logger.info("FINAL RESP::: " + finalsRes);
                        if (finalsRes == 1) {
                            switchGIPResponse.setError("00");
                            switchGIPResponse.setMessage("Success");
                        } else if (finalsRes == 2) {
                            switchGIPResponse.setError("06");
                            switchGIPResponse.setMessage("Already included or excluded, or must be taken from included or excluded data!!!");
                        } else {
                            switchGIPResponse.setError("06");
                            switchGIPResponse.setMessage("Update failed!!");
                        }

                    }
                }
            }
        } catch (SQLException e) {
            logger.info("Error::" + e.getMessage());
        } finally {
            try {
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException e3) {
                logger.info(" Sorry, something wrong!" + e3.getMessage());
            }
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e3) {
                logger.info(" Sorry, something wrong!" + e3.getMessage());
            }
        }

        return new JSONObject(switchGIPResponse).toString();

    }

    public int includeEmptyExcludeNotEmpty(SwitchGIPRequest switchGIPRequest) {
        Connection con = null;
        PreparedStatement ps = null;
        JSONObject switchReqJSON = new JSONObject(switchGIPRequest);
        logger.info("REQUEST RECEIVED:: " + switchReqJSON);
        int rs = -1;
        try {
            con = dataSource.getConnection();

            ps = con.prepareStatement("SELECT * FROM use_gip WHERE switch_key IN (?,?)");
            ps.setString(1, "GIP_INCLUDE_MERCHANTS");
            ps.setString(2, "GIP_EXCLUDE_MERCHANTS");
            ResultSet rs1 = ps.executeQuery();

            JSONArray requestArrayExcluded = null;
            String switch_value = "";
            String formattedRequestExclude = "";

            requestArrayExcluded = new JSONArray(switchGIPRequest.getExcludedBank());
            if (requestArrayExcluded.length() > 0) {
                while (rs1.next()) {
                    logger.info("RS: " + rs1);
                    switch_value += rs1.getString("switch_value") + "|";
                }
                for (int i = 0; i < requestArrayExcluded.length(); i++) {
                    if (!switch_value.contains(requestArrayExcluded.optString(i))) {
                        formattedRequestExclude += requestArrayExcluded.optString(i) + "#";
                    }

                }
                if (!formattedRequestExclude.isEmpty()) {
                    String sql = "UPDATE use_gip SET switch_value = ?"
                            + " WHERE switch_key = ?";
                    ps = con.prepareStatement(sql);
                    {
                        ps.setBoolean(1, Boolean.parseBoolean(switchGIPRequest.getSwitchToGIP()));
                        ps.setString(2, "USE_GIP");
                        rs = ps.executeUpdate();
                    }
                    {
                        ps.setString(1, switch_value.split("\\|")[0] + formattedRequestExclude);
                        ps.setString(2, "GIP_EXCLUDE_MERCHANTS");
                        rs = ps.executeUpdate();
                    }
                    logger.info("UPDATE RESULT:: " + rs);

                } else {
                    rs = 2;
                }

            }

        } catch (Exception e) {
            logger.info("Error::: " + e.getMessage());
        } finally {
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e3) {
                logger.info(" Sorry, something wrong!" + e3.getMessage());
            }
        }
        return rs;
    }

    public int includeNotEmptyExcludeEmpty(SwitchGIPRequest switchGIPRequest) {
        Connection con = null;
        PreparedStatement ps = null;
        JSONObject switchReqJSON = new JSONObject(switchGIPRequest);
        logger.info("REQUEST RECEIVED:: " + switchReqJSON);
        int rs = -1;
        try {
            con = dataSource.getConnection();

            ps = con.prepareStatement("SELECT * FROM use_gip WHERE switch_key IN (?,?)");
            ps.setString(1, "GIP_INCLUDE_MERCHANTS");
            ps.setString(2, "GIP_EXCLUDE_MERCHANTS");
            ResultSet rs1 = ps.executeQuery();

            JSONArray requestArrayIncluded = null;
            String switch_value = "";
            String formattedRequestInclude = "";

            requestArrayIncluded = new JSONArray(switchGIPRequest.getBankCode());
            if (requestArrayIncluded.length() > 0) {
                logger.info("HERE LENGTH > 0");
                while (rs1.next()) {
                    logger.info("RS: " + rs1);
                    switch_value += rs1.getString("switch_value") + "|";
                }

                for (int i = 0; i < requestArrayIncluded.length(); i++) {
                    if (!switch_value.contains(requestArrayIncluded.optString(i))) {
                        formattedRequestInclude += requestArrayIncluded.optString(i) + "#";
                    }

                }

                logger.info("SWITCH_VALUE::: " + switch_value);
                logger.info("REQUEST ADDITION:: " + formattedRequestInclude);
                if (!formattedRequestInclude.isEmpty()) {
                    String sql = "UPDATE use_gip SET switch_value = ?"
                            + " WHERE switch_key = ?";
                    logger.info("SQL::: " + sql);
                    ps = con.prepareStatement(sql);
                    if (switchGIPRequest.getSwitchToGIP().isEmpty()) {
                        {
                            ps.setString(1, switch_value.split("\\|")[0] + formattedRequestInclude);
                            ps.setString(2, "GIP_INCLUDE_MERCHANTS");
                            rs = ps.executeUpdate();
                        }
                    } else {
                        {
                            ps.setBoolean(1, Boolean.parseBoolean(switchGIPRequest.getSwitchToGIP()));
                            ps.setString(2, "USE_GIP");
                            rs = ps.executeUpdate();
                        }

                        {
                            ps.setString(1, switch_value.split("\\|")[0] + formattedRequestInclude);
                            ps.setString(2, "GIP_INCLUDE_MERCHANTS");
                            rs = ps.executeUpdate();
                        }
                    }
                    logger.info("UPDATE RESULT:: " + rs);

                } else {
                    rs = 2;
                }
            }

        } catch (Exception e) {
            logger.info("Error::: " + e.getMessage());
        } finally {
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e3) {
                logger.info(" Sorry, something wrong!" + e3.getMessage());
            }
        }
        return rs;
    }

    public int includeNotEmptyExcludeNotEmpty(SwitchGIPRequest switchGIPRequest) {
        Connection con = null;
        PreparedStatement ps = null;
        JSONObject switchReqJSON = new JSONObject(switchGIPRequest);
        logger.info("REQUEST RECEIVED:: " + switchReqJSON);

        int rs = 0;
        try {
            con = dataSource.getConnection();

            ps = con.prepareStatement("SELECT * FROM use_gip WHERE switch_key IN (?,?)");
            ps.setString(1, "GIP_INCLUDE_MERCHANTS");
            ps.setString(2, "GIP_EXCLUDE_MERCHANTS");
            ResultSet rs1 = ps.executeQuery();

            JSONArray requestArrayExcluded = null;
            JSONArray requestArrayIncluded = null;
            String switch_value = "";
            String formattedRequestExclude = "";
            String formattedRequestInclude = "";

            requestArrayExcluded = new JSONArray(switchGIPRequest.getExcludedBank());
            requestArrayIncluded = new JSONArray(switchGIPRequest.getBankCode());
            if (requestArrayExcluded.length() > 0 && requestArrayIncluded.length() > 0) {

                while (rs1.next()) {
                    logger.info("RS: " + rs1);
                    switch_value += rs1.getString("switch_value") + "|";
                }
                for (int i = 0; i < requestArrayExcluded.length(); i++) {
                    if (!switch_value.contains(requestArrayExcluded.optString(i))) {
                        formattedRequestExclude += requestArrayExcluded.optString(i) + "#";
                    }

                }
                for (int i = 0; i < requestArrayIncluded.length(); i++) {
                    if (!switch_value.contains(requestArrayIncluded.optString(i))) {
                        formattedRequestInclude += requestArrayIncluded.optString(i) + "#";
                    }

                }
                if (!formattedRequestInclude.isEmpty() && !formattedRequestExclude.isEmpty()){
                String sql = "UPDATE use_gip SET switch_value = ?"
                        + " WHERE switch_key = ?";
                ps = con.prepareStatement(sql);
                if (switchGIPRequest.getSwitchToGIP().isEmpty()) {
                    {

                        ps.setString(1, switch_value.split("\\|")[1] + formattedRequestInclude);
                        ps.setString(2, "GIP_INCLUDE_MERCHANTS");
                        rs = ps.executeUpdate();
                    }
                    {
                        ps.setString(1, switch_value.split("\\|")[0] + formattedRequestExclude);
                        ps.setString(2, "GIP_EXCLUDE_MERCHANTS");
                        rs = ps.executeUpdate();
                    }
                } else {
                    {
                        ps.setBoolean(1, Boolean.parseBoolean(switchGIPRequest.getSwitchToGIP()));
                        ps.setString(2, "USE_GIP");
                        rs = ps.executeUpdate();
                    }
                    {

                        ps.setString(1, switch_value.split("\\|")[1] + formattedRequestInclude);
                        ps.setString(2, "GIP_INCLUDE_MERCHANTS");
                        rs = ps.executeUpdate();
                    }
                    {
                        ps.setString(1, switch_value.split("\\|")[0] + formattedRequestExclude);
                        ps.setString(2, "GIP_EXCLUDE_MERCHANTS");
                        rs = ps.executeUpdate();
                    }
                }
                logger.info("UPDATE RESULT:: " + rs);


                }else {
                    rs = 2;
                }
            }

        } catch (Exception e) {
            logger.info("Error::: " + e.getMessage());
        } finally {
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e3) {
                logger.info(" Sorry, something wrong!" + e3.getMessage());
            }
        }
        return rs;
    }

    public int includeEmptyExcludeNotEmptyVoda(SwitchGIPRequest switchGIPRequest) {
        Connection con = null;
        PreparedStatement ps = null;
        JSONObject switchReqJSON = new JSONObject(switchGIPRequest);
        logger.info("REQUEST RECEIVED:: " + switchReqJSON);
        int rs = -1;
        try {
            con = dataSource.getConnection();

            ps = con.prepareStatement("SELECT * FROM use_gip WHERE switch_key IN (?,?)");
            ps.setString(1, "GIP_VODACASH_INCLUDE_MERCHANTS");
            ps.setString(2, "GIP_VODACASH_EXCLUDE_MERCHANTS");
            ResultSet rs1 = ps.executeQuery();

            JSONArray requestArrayExcluded = null;
            String switch_value = "";
            String formattedRequestExclude = "";

            requestArrayExcluded = new JSONArray(switchGIPRequest.getExcludedBank());
            if (requestArrayExcluded.length() > 0) {
                while (rs1.next()) {
                    logger.info("RS: " + rs1);
                    switch_value += rs1.getString("switch_value") + "|";
                }
                for (int i = 0; i < requestArrayExcluded.length(); i++) {
                    if (!switch_value.contains(requestArrayExcluded.optString(i))) {
                        formattedRequestExclude += requestArrayExcluded.optString(i) + "#";
                    }

                }
                if (!formattedRequestExclude.isEmpty()){
                String sql = "UPDATE use_gip SET switch_value = ?"
                        + " WHERE switch_key = ?";
                ps = con.prepareStatement(sql);
                if (switchGIPRequest.getSwitchToGIP().isEmpty()) {
                    {
                        ps.setString(1, switch_value.split("\\|")[0] + formattedRequestExclude);
                        ps.setString(2, "GIP_VODACASH_EXCLUDE_MERCHANTS");
                        rs = ps.executeUpdate();
                    }
                } else {
                    {
                        ps.setBoolean(1, Boolean.parseBoolean(switchGIPRequest.getSwitchToGIP()));
                        ps.setString(2, "USE_VODACASH_GIP");
                        rs = ps.executeUpdate();
                    }
                    {
                        ps.setString(1, switch_value.split("\\|")[0] + formattedRequestExclude);
                        ps.setString(2, "GIP_VODACASH_EXCLUDE_MERCHANTS");
                        rs = ps.executeUpdate();
                    }
                }
                logger.info("UPDATE RESULT:: " + rs);

                }else {
                    rs = 2;
                }
            }

        } catch (Exception e) {
            logger.info("Error::: " + e.getMessage());
        } finally {
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e3) {
                logger.info(" Sorry, something wrong!" + e3.getMessage());
            }
        }
        return rs;
    }

    public int includeNotEmptyExcludeEmptyVoda(SwitchGIPRequest switchGIPRequest) {
        Connection con = null;
        PreparedStatement ps = null;
        JSONObject switchReqJSON = new JSONObject(switchGIPRequest);
        logger.info("REQUEST RECEIVED:: " + switchReqJSON);
        int rs = -1;
        try {
            con = dataSource.getConnection();

            ps = con.prepareStatement("SELECT * FROM use_gip WHERE switch_key IN (?,?)");
            ps.setString(1, "GIP_VODACASH_INCLUDE_MERCHANTS");
            ps.setString(2, "GIP_VODACASH_EXCLUDE_MERCHANTS");
            ResultSet rs1 = ps.executeQuery();

            JSONArray requestArrayIncluded = null;
            String switch_value = "";
            String formattedRequestInclude = "";

            requestArrayIncluded = new JSONArray(switchGIPRequest.getBankCode());
            if (requestArrayIncluded.length() > 0) {
                logger.info("HERE LENGTH > 0");
                while (rs1.next()) {
                    logger.info("RS: " + rs1);
                    switch_value += rs1.getString("switch_value") + "|";
                }
                logger.info("SWITCH VALUE::: " + switch_value);
                logger.info("REQUESTED INCLUDE:: " + requestArrayIncluded);
                for (int i = 0; i < requestArrayIncluded.length(); i++) {
                    if (!switch_value.contains(requestArrayIncluded.optString(i))) {
                        formattedRequestInclude += requestArrayIncluded.optString(i) + "#";
                    }

                }

                if (!formattedRequestInclude.isEmpty()) {
                    logger.info("FORMATTED INCLUDE::: " + formattedRequestInclude);
                    String sql = "UPDATE use_gip SET switch_value = ?"
                            + " WHERE switch_key = ?";
                    logger.info("SQL::: " + sql);
                    ps = con.prepareStatement(sql);
                    if (switchGIPRequest.getSwitchToGIP().isEmpty()) {
                        logger.info("I'm in here, switch is empty");
                        ps.setString(1, switch_value.split("\\|")[0] + formattedRequestInclude);
                        ps.setString(2, "GIP_VODACASH_INCLUDE_MERCHANTS");
                        rs = ps.executeUpdate();

                    } else {
                        {
                            ps.setBoolean(1, Boolean.parseBoolean(switchGIPRequest.getSwitchToGIP()));
                            ps.setString(2, "USE_VODACASH_GIP");
                            rs = ps.executeUpdate();
                        }
                        {
                            ps.setString(1, switch_value.split("\\|")[0] + formattedRequestInclude);
                            ps.setString(2, "GIP_VODACASH_INCLUDE_MERCHANTS");
                            rs = ps.executeUpdate();
                        }
                    }
                    logger.info("UPDATE RESULT:: " + rs);

                } else {
                    rs = 2;
                }
            }

        } catch (Exception e) {
            logger.info("Error::: " + e.getMessage());
        } finally {
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e3) {
                logger.info(" Sorry, something wrong!" + e3.getMessage());
            }
        }
        return rs;
    }

    public int includeNotEmptyExcludeNotEmptyVoda(SwitchGIPRequest switchGIPRequest) {
        Connection con = null;
        PreparedStatement ps = null;
        JSONObject switchReqJSON = new JSONObject(switchGIPRequest);
        logger.info("REQUEST RECEIVED:: " + switchReqJSON);

        int rs = 0;
        try {
            con = dataSource.getConnection();

            ps = con.prepareStatement("SELECT * FROM use_gip WHERE switch_key IN (?,?)");
            ps.setString(1, "GIP_VODACASH_INCLUDE_MERCHANTS");
            ps.setString(2, "GIP_VODACASH_EXCLUDE_MERCHANTS");
            ResultSet rs1 = ps.executeQuery();

            JSONArray requestArrayExcluded = null;
            JSONArray requestArrayIncluded = null;
            String switch_value = "";
            String formattedRequestExclude = "";
            String formattedRequestInclude = "";

            requestArrayExcluded = new JSONArray(switchGIPRequest.getExcludedBank());
            requestArrayIncluded = new JSONArray(switchGIPRequest.getBankCode());
            if (requestArrayExcluded.length() > 0 && requestArrayIncluded.length() > 0) {
                while (rs1.next()) {
                    logger.info("RS: " + rs1);
                    switch_value += rs1.getString("switch_value") + "|";
                }
                for (int i = 0; i < requestArrayExcluded.length(); i++) {
                    if (!switch_value.contains(requestArrayExcluded.optString(i))) {
                        formattedRequestExclude += requestArrayExcluded.optString(i) + "#";
                    }

                }
                for (int i = 0; i < requestArrayIncluded.length(); i++) {
                    if (!switch_value.contains(requestArrayIncluded.optString(i))) {
                        formattedRequestInclude += requestArrayIncluded.optString(i) + "#";
                    }

                }
                if (!formattedRequestInclude.isEmpty() && !formattedRequestExclude.isEmpty()){
                String sql = "UPDATE use_gip SET switch_value = ?"
                        + " WHERE switch_key = ?";
                ps = con.prepareStatement(sql);
                if (switchGIPRequest.getSwitchToGIP().isEmpty()) {
                    {

                        ps.setString(1, switch_value.split("\\|")[1] + formattedRequestInclude);
                        ps.setString(2, "GIP_VODACASH_INCLUDE_MERCHANTS");
                        rs = ps.executeUpdate();
                    }
                    {
                        ps.setString(1, switch_value.split("\\|")[0] + formattedRequestExclude);
                        ps.setString(2, "GIP_VODACASH_EXCLUDE_MERCHANTS");
                        rs = ps.executeUpdate();
                    }
                } else {
                    {
                        ps.setBoolean(1, Boolean.parseBoolean(switchGIPRequest.getSwitchToGIP()));
                        ps.setString(2, "USE_GIP");
                        rs = ps.executeUpdate();
                    }
                    {

                        ps.setString(1, switch_value.split("\\|")[1] + formattedRequestInclude);
                        ps.setString(2, "GIP_VODACASH_INCLUDE_MERCHANTS");
                        rs = ps.executeUpdate();
                    }
                    {
                        ps.setString(1, switch_value.split("\\|")[0] + formattedRequestExclude);
                        ps.setString(2, "GIP_VODACASH_EXCLUDE_MERCHANTS");
                        rs = ps.executeUpdate();
                    }
                }
                logger.info("UPDATE RESULT:: " + rs);


                }else {
                    rs = 2;
                }
            }

        } catch (Exception e) {
            logger.info("Error::: " + e.getMessage());
        } finally {
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e3) {
                logger.info(" Sorry, something wrong!" + e3.getMessage());
            }
        }
        return rs;
    }

    public int includeEmptyExcludeNotEmptyAirtelTigo(SwitchGIPRequest switchGIPRequest) {
        Connection con = null;
        PreparedStatement ps = null;
        JSONObject switchReqJSON = new JSONObject(switchGIPRequest);
        logger.info("REQUEST RECEIVED:: " + switchReqJSON);
        int rs = -1;
        try {
            con = dataSource.getConnection();

            ps = con.prepareStatement("SELECT * FROM use_gip WHERE switch_key IN (?,?)");
            ps.setString(1, "GIP_AIRTELTIGO_INCLUDE_MERCHANTS");
            ps.setString(2, "GIP_AIRTELTIGO_EXCLUDE_MERCHANTS");
            ResultSet rs1 = ps.executeQuery();

            JSONArray requestArrayExcluded = null;
            String switch_value = "";
            String formattedRequestExclude = "";

            requestArrayExcluded = new JSONArray(switchGIPRequest.getExcludedBank());
            if (requestArrayExcluded.length() > 0) {
                while (rs1.next()) {
                    logger.info("RS: " + rs1);
                    switch_value += rs1.getString("switch_value") + "|";
                }
                for (int i = 0; i < requestArrayExcluded.length(); i++) {
                    if (!switch_value.contains(requestArrayExcluded.optString(i))) {
                        formattedRequestExclude += requestArrayExcluded.optString(i) + "#";
                    }

                }
                if (!formattedRequestExclude.isEmpty()){
                String sql = "UPDATE use_gip SET switch_value = ?"
                        + " WHERE switch_key = ?";
                ps = con.prepareStatement(sql);
                if (switchGIPRequest.getSwitchToGIP().isEmpty()) {
                    {
                        ps.setString(1, switch_value.split("\\|")[0] + formattedRequestExclude);
                        ps.setString(2, "GIP_AIRTELTIGO_EXCLUDE_MERCHANTS");
                        rs = ps.executeUpdate();
                    }
                } else {
                    {
                        ps.setBoolean(1, Boolean.parseBoolean(switchGIPRequest.getSwitchToGIP()));
                        ps.setString(2, "USE_AIRTELTIGO_GIP");
                        rs = ps.executeUpdate();
                    }
                    {
                        ps.setString(1, switch_value.split("\\|")[0] + formattedRequestExclude);
                        ps.setString(2, "GIP_AIRTELTIGO_EXCLUDE_MERCHANTS");
                        rs = ps.executeUpdate();
                    }
                }

                logger.info("UPDATE RESULT:: " + rs);

            }else {
                    rs = 2;
                }
            }

        } catch (Exception e) {
            logger.info("Error::: " + e.getMessage());
        } finally {
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e3) {
                logger.info(" Sorry, something wrong!" + e3.getMessage());
            }
        }
        return rs;
    }

    public int updateOnlySwitch(SwitchGIPRequest switchGIPRequest) {
        Connection con = null;
        PreparedStatement ps = null;
        JSONObject switchReqJSON = new JSONObject(switchGIPRequest);
        logger.info("REQUEST RECEIVED:: " + switchReqJSON);
        int rs = -1;
        try {
            con = dataSource.getConnection();

            String sql = "UPDATE use_gip SET switch_value = ?"
                    + " WHERE switch_key = ?";
            ps = con.prepareStatement(sql);
            if (switchGIPRequest.getNetwork().equals("844")) {
                ps.setBoolean(1, Boolean.parseBoolean(switchGIPRequest.getSwitchToGIP()));
                ps.setString(2, "USE_AIRTELTIGO_GIP");
                rs = ps.executeUpdate();
            } else if (switchGIPRequest.getNetwork().equals("863")) {
                ps.setBoolean(1, Boolean.parseBoolean(switchGIPRequest.getSwitchToGIP()));
                ps.setString(2, "USE_VODACASH_GIP");
                rs = ps.executeUpdate();
            } else if (switchGIPRequest.getNetwork().equals("686")) {
                ps.setBoolean(1, Boolean.parseBoolean(switchGIPRequest.getSwitchToGIP()));
                ps.setString(2, "USE_GIP");
                rs = ps.executeUpdate();
            }
            logger.info("UPDATE RESULT:: " + rs);


        } catch (Exception e) {
            logger.info("Error::: " + e.getMessage());
        } finally {
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e3) {
                logger.info(" Sorry, something wrong!" + e3.getMessage());
            }
        }
        return rs;
    }

    public int includeNotEmptyExcludeEmptyAirtelTigo(SwitchGIPRequest switchGIPRequest) {
        Connection con = null;
        PreparedStatement ps = null;
        JSONObject switchReqJSON = new JSONObject(switchGIPRequest);
        logger.info("REQUEST RECEIVED:: " + switchReqJSON);
        int rs = -1;
        try {
            con = dataSource.getConnection();

            ps = con.prepareStatement("SELECT * FROM use_gip WHERE switch_key IN (?,?)");
            ps.setString(1, "GIP_AIRTELTIGO_INCLUDE_MERCHANTS");
            ps.setString(2, "GIP_AIRTELTIGO_EXCLUDE_MERCHANTS");
            ResultSet rs1 = ps.executeQuery();

            JSONArray requestArrayIncluded = null;
            String switch_value = "";
            String formattedRequestInclude = "";

            long start = System.currentTimeMillis();

            requestArrayIncluded = new JSONArray(switchGIPRequest.getBankCode());
            if (requestArrayIncluded.length() > 0) {
                logger.info("HERE LENGTH > 0");

                while (rs1.next()) {
                    logger.info("RS: " + rs1);
                    switch_value += rs1.getString("switch_value") + "|";
                }
                for (int i = 0; i < requestArrayIncluded.length(); i++) {
                    if (!switch_value.contains(requestArrayIncluded.optString(i))) {
                        formattedRequestInclude += requestArrayIncluded.optString(i) + "#";
                    }

                }
                logger.info("SWITCH_VALUE::: " + switch_value);

                if (!formattedRequestInclude.isEmpty()){
                String sql = "UPDATE use_gip SET switch_value = ?"
                        + " WHERE switch_key = ?";
                logger.info("SQL::: " + sql);
                ps = con.prepareStatement(sql);
                if (switchGIPRequest.getSwitchToGIP().isEmpty()) {
                    {
                        ps.setString(1, switch_value.split("\\|")[0] + formattedRequestInclude);
                        ps.setString(2, "GIP_AIRTELTIGO_INCLUDE_MERCHANTS");
                        rs = ps.executeUpdate();
                    }
                } else {
                    {
                        ps.setBoolean(1, Boolean.parseBoolean(switchGIPRequest.getSwitchToGIP()));
                        ps.setString(2, "USE_AIRTELTIGO_GIP");
                        rs = ps.executeUpdate();
                    }
                    {
                        ps.setString(1, switch_value.split("\\|")[0] + formattedRequestInclude);
                        ps.setString(2, "GIP_AIRTELTIGO_INCLUDE_MERCHANTS");
                        rs = ps.executeUpdate();
                    }
                }
                logger.info("UPDATE RESULT:: " + rs);

            }else {
                    rs = 2;
                }
            }

        } catch (Exception e) {
            logger.info("Error::: " + e.getMessage());
        } finally {
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e3) {
                logger.info(" Sorry, something wrong!" + e3.getMessage());
            }
        }
        return rs;
    }

    public int includeNotEmptyExcludeNotEmptyAirtelTigo(SwitchGIPRequest switchGIPRequest) {
        SwitchGIPResponse switchGIPResponse = new SwitchGIPResponse();
        Connection con = null;
        PreparedStatement ps = null;
        JSONObject switchReqJSON = new JSONObject(switchGIPRequest);
        logger.info("REQUEST RECEIVED:: " + switchReqJSON);

        int rs = 0;
        try {
            con = dataSource.getConnection();

            ps = con.prepareStatement("SELECT * FROM use_gip WHERE switch_key IN (?,?)");
            ps.setString(1, "GIP_AIRTELTIGO_INCLUDE_MERCHANTS");
            ps.setString(2, "GIP_AIRTELTIGO_EXCLUDE_MERCHANTS");
            ResultSet rs1 = ps.executeQuery();

            JSONArray requestArrayExcluded = null;
            JSONArray requestArrayIncluded = null;
            String switch_value = "";
            String formattedRequestExclude = "";
            String formattedRequestInclude = "";
            long start = System.currentTimeMillis();

            requestArrayExcluded = new JSONArray(switchGIPRequest.getExcludedBank());
            requestArrayIncluded = new JSONArray(switchGIPRequest.getBankCode());
            if (requestArrayExcluded.length() > 0 && requestArrayIncluded.length() > 0) {
                while (rs1.next()) {
                    logger.info("RS: " + rs1);
                    switch_value += rs1.getString("switch_value") + "|";
                }
                for (int i = 0; i < requestArrayExcluded.length(); i++) {
                    if (!switch_value.contains(requestArrayExcluded.optString(i))) {
                        formattedRequestExclude += requestArrayExcluded.optString(i) + "#";
                    }

                }
                for (int i = 0; i < requestArrayIncluded.length(); i++) {
                    if (!switch_value.contains(requestArrayIncluded.optString(i))) {
                        formattedRequestInclude += requestArrayIncluded.optString(i) + "#";
                    }

                }

                if (!formattedRequestInclude.isEmpty() && !formattedRequestExclude.isEmpty()) {
                    String sql = "UPDATE use_gip SET switch_value = ?"
                            + " WHERE switch_key = ?";
                    ps = con.prepareStatement(sql);
                    if (switchGIPRequest.getSwitchToGIP().isEmpty()) {
                        {

                            ps.setString(1, switch_value.split("\\|")[1] + formattedRequestInclude);
                            ps.setString(2, "GIP_AIRTELTIGO_INCLUDE_MERCHANTS");
                            rs = ps.executeUpdate();
                        }
                        {
                            ps.setString(1, switch_value.split("\\|")[0] + formattedRequestExclude);
                            ps.setString(2, "GIP_AIRTELTIGO_EXCLUDE_MERCHANTS");
                            rs = ps.executeUpdate();
                        }
                    } else {
                        {
                            ps.setBoolean(1, Boolean.parseBoolean(switchGIPRequest.getSwitchToGIP()));
                            ps.setString(2, "USE_AIRTELTIGO_GIP");
                            rs = ps.executeUpdate();
                        }
                        {

                            ps.setString(1, switch_value.split("\\|")[1] + formattedRequestInclude);
                            ps.setString(2, "GIP_AIRTELTIGO_INCLUDE_MERCHANTS");
                            rs = ps.executeUpdate();
                        }
                        {
                            ps.setString(1, switch_value.split("\\|")[0] + formattedRequestExclude);
                            ps.setString(2, "GIP_AIRTELTIGO_EXCLUDE_MERCHANTS");
                            rs = ps.executeUpdate();
                        }
                        long end = System.currentTimeMillis();
                        System.out.println("->DB TAT" + (end - start));
                        logger.info("->DB TAT" + (end - start));
                        switchGIPResponse.setTat((end - start));
                        logger.info("UPDATE RESULT:: " + rs);


                    }
                } else {
                    rs = 2;
                }
            }

        } catch (Exception e) {
            logger.info("Error::: " + e.getMessage());
        } finally {
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e3) {
                logger.info(" Sorry, something wrong!" + e3.getMessage());
            }
        }
        return rs;
    }

}