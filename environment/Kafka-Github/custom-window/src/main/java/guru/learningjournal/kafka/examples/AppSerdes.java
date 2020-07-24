/*
 * Copyright (c) 2019. Prashant Kumar Pandey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.JsonDeserializer;
import guru.learningjournal.kafka.examples.serde.JsonSerializer;
import guru.learningjournal.kafka.examples.types.StockTicker;
import guru.learningjournal.kafka.examples.types.TickerStack;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.HashMap;
import java.util.Map;


class AppSerdes extends Serdes {


    static final class StockTickerSerde extends WrapperSerde<StockTicker> {
        StockTickerSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    static Serde<StockTicker> StockTicker() {
        StockTickerSerde serde = new StockTickerSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put("specific.class.name", StockTicker.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }

    static final class TickerStackSerde extends WrapperSerde<TickerStack> {
        TickerStackSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    static Serde<TickerStack> TickerStack() {
        TickerStackSerde serde = new TickerStackSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put("specific.class.name", TickerStack.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }

}