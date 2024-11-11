package org.apache.flamingo.memtable.skiplist;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flamingo.utils.StringUtil;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

@Slf4j
public class SkipList {

    private SLNode head;

    private SLNode tail;

    @Getter
    private int size;

    private int level;

    private final Random random = new Random();

    private final double probability;

    private final int maxLevel = 1;

    public SkipList(double probability) {
        this.head = new SLNode(SLNode.HeadKey, null);
        this.tail = new SLNode(SLNode.TailKey, null);
        this.size = 0;
        this.level = 0;
        this.probability = probability;
        horizontal(head, tail);
    }

    public SkipList() {
        this(0.5);
    }

    public void put(String key, String value) {
        log.debug("Put Key: {}", key);
        put(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    public void put(byte[] key, byte[] value) {
        SLNode anchorNode = findPrev(key);
        // 目标节点存在
        if (Arrays.equals(key, anchorNode.getKey())) {
            anchorNode.setValue(value);
            return;
        }
        SLNode insertNode = new SLNode(key, value);
        afterInsert(anchorNode, insertNode);
        int currentLevel = 0;
        // 判断是否需要对节点进行升级建立索引层
        while (random.nextDouble() < probability) {
            // If it exceeds the height, a new top floor needs to be built
            if (currentLevel >= level && currentLevel < maxLevel) {
                level++;
                SLNode newHead = new SLNode(SLNode.HeadKey, null);
                SLNode newTail = new SLNode(SLNode.TailKey, null);
                horizontal(newHead, newTail);
                vertical(newHead, head);
                vertical(newTail, tail);
                head = newHead;
                tail = newTail;
            }
            // 此处有可能被移动到HEAD节点所以需要加getLeft()判断
            while (anchorNode != null && anchorNode.getUp() == null) {
                anchorNode = anchorNode.getLeft();
            }
            if (anchorNode == null) {
                break;
            }
            anchorNode = anchorNode.getUp();
            SLNode e = new SLNode(key, null);
            afterInsert(anchorNode, e);
            vertical(e, insertNode);
            insertNode = e;
            currentLevel++;
        }
        size++;
        check();
    }

    public void remove(String key) {
        log.debug("Remove key: {}", key);
        remove(key.getBytes(StandardCharsets.UTF_8));
        check();
    }

    public void remove(byte[] key) {
        SLNode anchorNode = findPrev(key);
        // anchorNode有两种情况:
        // 1: 目标节点存在则anchorNode一定是key
        // 2: 目标节点不存在则anchorNode一定是Tail前面一个元素
        if (!Arrays.equals(anchorNode.getKey(), key)) {
            return;
        }
        anchorNode = anchorNode.getLeft();
        while (anchorNode != null) {
            SLNode toRemove = anchorNode.getRight();
            if (!Arrays.equals(key, toRemove.getKey())) {
                break;
            }
            anchorNode.setRight(toRemove.getRight());
            toRemove.getRight().setLeft(anchorNode);
            // 移动到左侧第一个携带上层节点的节点
            while (anchorNode.getLeft() != null && anchorNode.getUp() == null) {
                anchorNode = anchorNode.getLeft();
            }
            anchorNode = anchorNode.getUp();
            if (toRemove.getDown() != null) {
                toRemove.getDown().setUp(null);
            }
            toRemove.setDown(null);
            toRemove.setRight(null);
            toRemove.setLeft(null);
        }
        size--;
        while (level > 1 && head.getRight().getKey() == SLNode.TailKey) {
            head = head.getDown();
            tail = tail.getDown();
            level--;
        }
    }

    public byte[] search(String key) {
        return search(key.getBytes(StandardCharsets.UTF_8));
    }

    public byte[] search(byte[] key) {
        SLNode p = findPrev(key);
        if (Arrays.equals(key, p.getKey())) {
            return p.getValue();
        } else {
            return null;
        }
    }

    /**
     * Find the node in front of the position to be inserted at the bottom layer. <p>
     * newHead会出现一下情况: <p>
     * 1: head节点上 (没有节点或者小于第一个节点) <p>
     * 2: 目标节点上 (key在链表中存在) <p>
     * 3: 目标节点的前一个节点 (key在链表中不存在)
     */
    private SLNode findPrev(byte[] key) {
        SLNode newHead = head;
        while (true) {
            if (newHead.getRight() == null) {
                log.debug("NullPointException");
            }
            while (!Arrays.equals(newHead.getRight().getKey(), SLNode.TailKey) && compareByteArrays(newHead.getRight().getKey(), key) <= 0) {
                newHead = newHead.getRight();
            }
            if (newHead.getDown() != null) {
                newHead = newHead.getDown();
            } else {
                break;
            }
        }
        return newHead;
    }

    private void afterInsert(SLNode anchor, SLNode insert) {
        insert.setLeft(anchor);
        insert.setRight(anchor.getRight());
        anchor.getRight().setLeft(insert);
        anchor.setRight(insert);
    }

    private void horizontal(SLNode prev, SLNode next) {
        prev.setRight(next);
        next.setLeft(prev);
    }

    private void vertical(SLNode up, SLNode down) {
        up.setDown(down);
        down.setUp(up);
    }

    public static int compareByteArrays(byte[] array1, byte[] array2) {
        int minLength = Math.min(array1.length, array2.length);
        for (int i = 0; i < minLength; i++) {
            if (array1[i] != array2[i]) {
                return Byte.compare(array1[i], array2[i]);
            }
        }
        // If we reach here, the arrays are equal up to the length of the shorter one.
        // The longer array is considered greater.
        return Integer.compare(array1.length, array2.length);
    }

    public SLNode getLastHead() {
        SLNode p = head;
        while (p.getDown() != null) {
            p = p.getDown();
        }
        return p;
    }

    private void check() {
        SLNode lastHead = getLastHead();
        while (lastHead != null) {
            SLNode tmp = lastHead;
            while (tmp.getUp() != null) {
                if(!Arrays.equals(tmp.getUp().getKey(), tmp.getKey())) {
                    throw new RuntimeException("Inconsistent elements");
                }
                tmp = tmp.getUp();
            }
            lastHead = lastHead.getRight();
        }
    }

    @Override
    public String toString() {
        if (size == 0) {
            return "跳跃表为空！";
        }
        StringBuilder builder = new StringBuilder();
        SLNode p = head;
        while (p.getDown() != null) {
            p = p.getDown();
        }
        while (p.getLeft() != null) {
            p = p.getLeft();
        }
        if (p.getRight() != null) {
            p = p.getRight();
        }
        while (p.getRight() != null) {
            builder.append(p);
            builder.append("\n");
            p = p.getRight();
        }
        return builder.toString();
    }

    public List<String> keys() {
        SLNode lastHead = getLastHead();
        lastHead = lastHead.getRight();
        List<String> keys = new ArrayList<>();
        while (lastHead.getRight() != null) {
            keys.add(new String(lastHead.getKey(), StandardCharsets.UTF_8));
            lastHead = lastHead.getRight();
        }
        return keys;
    }

    public String graphSKipList() {
        StringBuilder builder = new StringBuilder();
        ArrayList<List<String>> vertical = new ArrayList<>();
        SLNode lastHead = getLastHead();
        int maxWidth = 0;
        while (lastHead != null) {
            SLNode currentNode = lastHead;
            ArrayList<String> itemArray = new ArrayList<>();
            String lastWord = null;
            for (int i = 0; i < level; i++) {
                if (currentNode == null) {
                    itemArray.add("<" + lastWord + ">");
                } else {
                    if (lastWord == null) {
                        lastWord = StringUtil.fromBytes(currentNode.getKey());
                    }
                    maxWidth = Math.max(maxWidth, lastWord.length());
                    itemArray.add(lastWord);
                    currentNode = currentNode.getUp();
                }
            }
            lastHead = lastHead.getRight();
            vertical.add(itemArray);
        }
        final int finalWidth = maxWidth + 5;
        for (int i = level - 1; i >= 0; i--) {
            for (List<String> line : vertical) {
                builder.append(getPaddingString(finalWidth, line.get(i)));
            }
            builder.append("\n");
        }
        return builder.toString();
    }

    private String getPaddingString(int totalWith, String str) {
        int padding = (totalWith - str.length()) / 2;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < padding; i++) {
            sb.append(' ');
        }
        sb.append(str);
        for (int i = 0; i < padding; i++) {
            sb.append(' ');
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        String str = "HelloWorld";
        int totalLength = 20;
        // 计算需要添加的空格数
        int padding = (totalLength - str.length()) / 2;
        // 使用String.format()格式化字符串
        String formattedStr = String.format("%" + (padding + str.length()) + "s %" + padding + "s", "", str);
        System.out.println(formattedStr);
    }

}
