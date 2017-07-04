package org.neo4j.driver.internal.cluster;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RoundRobinIndexTest
{
    @Test
    public void shouldHandleZeroLength()
    {
        RoundRobinIndex roundRobinIndex = new RoundRobinIndex();

        int index = roundRobinIndex.next( 0 );

        assertEquals( -1, index );
    }

    @Test
    public void shouldReturnIndexesInRoundRobinOrder()
    {
        RoundRobinIndex roundRobinIndex = new RoundRobinIndex();

        for ( int i = 0; i < 10; i++ )
        {
            int index = roundRobinIndex.next( 10 );
            assertEquals( i, index );
        }

        for ( int i = 0; i < 5; i++ )
        {
            int index = roundRobinIndex.next( 5 );
            assertEquals( i, index );
        }
    }

    @Test
    public void shouldHandleOverflow()
    {
        int arrayLength = 10;
        RoundRobinIndex roundRobinIndex = new RoundRobinIndex( Integer.MAX_VALUE - 1 );

        assertEquals( (Integer.MAX_VALUE - 1) % arrayLength, roundRobinIndex.next( arrayLength ) );
        assertEquals( Integer.MAX_VALUE % arrayLength, roundRobinIndex.next( arrayLength ) );
        assertEquals( 0, roundRobinIndex.next( arrayLength ) );
        assertEquals( 1, roundRobinIndex.next( arrayLength ) );
        assertEquals( 2, roundRobinIndex.next( arrayLength ) );
    }
}
