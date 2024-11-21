package org.apache.flamingo.bean;

import lombok.*;

import java.nio.ByteBuffer;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class VLogAddress {

    private Long fieldID;

    private Long offset;

    public static VLogAddress from(Long fieldID, Long offset) {
        return new VLogAddress(fieldID, offset);
    }

    public static byte[] serialize(VLogAddress address) {
        ByteBuffer buffer = ByteBuffer.allocate(8 + 8);
        buffer.putLong(address.fieldID);
        buffer.putLong(address.offset);
        return buffer.array();
    }

}
