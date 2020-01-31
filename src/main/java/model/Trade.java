package model;

import lombok.Data;

import java.math.BigDecimal;
import java.sql.Timestamp;

@Data
public class Trade {

    private String tradeReference;
    private String accountNumber;
    private String stockCode;
    private BigDecimal quantity;
    private String currency;
    private BigDecimal price;
    private String broker;
    private BigDecimal amount;
    private Timestamp receivedTimestamp;

}
