package gobblin.source.extractor.extract.kafka;

import lombok.Getter;

@Getter
public class KafkaRecord implements Comparable<KafkaRecord> {

    private final Long offset;
    private final String key;
    private final String payload;

    public KafkaRecord(long offset, String key, String payload) {
        super();
        this.offset = offset;
        this.key = key;
        this.payload = payload;
    }

    @Override
    public String toString() {
        return "KafkaRecord [offset=" + offset + ", key=" + key + ", payload=" + payload + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        result = prime * result + (int) (offset ^ (offset >>> 32));
        result = prime * result + ((payload == null) ? 0 : payload.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        KafkaRecord other = (KafkaRecord) obj;
        if (key == null) {
            if (other.key != null) {
                return false;
            }
        } else if (!key.equals(other.key)) {
            return false;
        }
        if (offset != other.offset) {
            return false;
        }
        if (payload == null) {
            if (other.payload != null) {
                return false;
            }
        } else if (!payload.equals(other.payload)) {
            return false;
        }
        return true;
    }

    public int compareTo(KafkaRecord o) {
        return this.offset.compareTo(o.offset);
    }

}
