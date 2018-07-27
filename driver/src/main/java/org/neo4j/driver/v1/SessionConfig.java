/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.v1;

import java.util.Objects;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;

public class SessionConfig
{
    private final AccessMode accessMode;
    private final Iterable<String> bookmarks;
    private final String database;

    private SessionConfig( Builder builder )
    {
        this.accessMode = builder.accessMode;
        this.bookmarks = builder.bookmarks;
        this.database = builder.database;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public AccessMode accessMode()
    {
        return accessMode;
    }

    public Iterable<String> bookmarks()
    {
        return bookmarks;
    }

    public String database()
    {
        return database;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        SessionConfig that = (SessionConfig) o;
        return accessMode == that.accessMode &&
               Objects.equals( bookmarks, that.bookmarks ) &&
               Objects.equals( database, that.database );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( accessMode, bookmarks, database );
    }

    @Override
    public String toString()
    {
        return "SessionConfig{" +
               "accessMode=" + accessMode +
               ", bookmarks=" + bookmarks +
               ", database='" + database + '\'' +
               '}';
    }

    public static class Builder
    {
        private AccessMode accessMode = AccessMode.WRITE;
        private Iterable<String> bookmarks = emptyList();
        private String database;

        private Builder()
        {
        }

        public Builder withAccessMode( AccessMode accessMode )
        {
            Objects.requireNonNull( accessMode, "Access mode should not be null" );
            this.accessMode = accessMode;
            return this;
        }

        public Builder withBookmark( String bookmark )
        {
            Objects.requireNonNull( bookmark, "Bookmark should not be null" );
            return withBookmarks( singleton( bookmark ) );
        }

        public Builder withBookmarks( Iterable<String> bookmarks )
        {
            Objects.requireNonNull( accessMode, "Bookmarks should not be null" );
            this.bookmarks = bookmarks;
            return this;
        }

        public Builder withDatabase( String database )
        {
            Objects.requireNonNull( database, "Database should not be null" );
            this.database = database;
            return this;
        }

        public SessionConfig build()
        {
            return new SessionConfig( this );
        }
    }
}
