package org.neo4j.driver.v1;

public interface TransactionWork<T, U extends Throwable>
{
    T execute( Transaction tx ) throws U;
}
