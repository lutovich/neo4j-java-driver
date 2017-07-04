package org.neo4j.driver.internal.cluster;

import org.junit.Test;

import org.neo4j.driver.internal.net.BoltServerAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RoundRobinLoadBalancingStrategyTest
{
    private final RoundRobinLoadBalancingStrategy strategy = new RoundRobinLoadBalancingStrategy();

    @Test
    public void shouldHandleEmptyReadersArray()
    {
        assertNull( strategy.selectReader( new BoltServerAddress[0] ) );
    }

    @Test
    public void shouldHandleEmptyWritersArray()
    {
        assertNull( strategy.selectWriter( new BoltServerAddress[0] ) );
    }

    @Test
    public void shouldHandleSingleReader()
    {
        BoltServerAddress address = new BoltServerAddress( "reader", 9999 );

        assertEquals( address, strategy.selectReader( new BoltServerAddress[]{address} ) );
    }

    @Test
    public void shouldHandleSingleWriter()
    {
        BoltServerAddress address = new BoltServerAddress( "writer", 9999 );

        assertEquals( address, strategy.selectWriter( new BoltServerAddress[]{address} ) );
    }

    @Test
    public void shouldReturnReadersInRoundRobinOrder()
    {
        BoltServerAddress address1 = new BoltServerAddress( "server-1", 1 );
        BoltServerAddress address2 = new BoltServerAddress( "server-2", 2 );
        BoltServerAddress address3 = new BoltServerAddress( "server-3", 3 );
        BoltServerAddress address4 = new BoltServerAddress( "server-4", 4 );

        BoltServerAddress[] readers = {address1, address2, address3, address4};

        assertEquals( address1, strategy.selectReader( readers ) );
        assertEquals( address2, strategy.selectReader( readers ) );
        assertEquals( address3, strategy.selectReader( readers ) );
        assertEquals( address4, strategy.selectReader( readers ) );

        assertEquals( address1, strategy.selectReader( readers ) );
        assertEquals( address2, strategy.selectReader( readers ) );
        assertEquals( address3, strategy.selectReader( readers ) );
        assertEquals( address4, strategy.selectReader( readers ) );
    }

    @Test
    public void shouldReturnWriterInRoundRobinOrder()
    {
        BoltServerAddress address1 = new BoltServerAddress( "server-1", 1 );
        BoltServerAddress address2 = new BoltServerAddress( "server-2", 2 );
        BoltServerAddress address3 = new BoltServerAddress( "server-3", 3 );

        BoltServerAddress[] writers = {address1, address2, address3};

        assertEquals( address1, strategy.selectWriter( writers ) );
        assertEquals( address2, strategy.selectWriter( writers ) );
        assertEquals( address3, strategy.selectWriter( writers ) );

        assertEquals( address1, strategy.selectWriter( writers ) );
        assertEquals( address2, strategy.selectWriter( writers ) );
        assertEquals( address3, strategy.selectWriter( writers ) );
    }
}
