# Campaign Demo

```mermaid
graph LR;
    join1[/Create Table/]
    DebitCardTxns-->join1;
    Customer-->join1;
    join1 --> CashbackEarned
    CashbackOrder-->TotalCashbackOrder;
    TotalCashbackOrder-->EarnedOrderDiff;
    CashbackEarned-->EarnedOrderDiff;
    EarnedOrderDiff-->CashbackOrder;
```
