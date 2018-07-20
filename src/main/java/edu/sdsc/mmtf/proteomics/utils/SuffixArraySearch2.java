package edu.sdsc.mmtf.proteomics.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class SuffixArraySearch2<T> implements Serializable {
	private static final long serialVersionUID = -5295504758136411632L;
	private List<T> keys;
	private List<String> text;
	private SuffixArrayX suffix;
	private int[] indices;

	/**
	 * Constructs a suffix array for a map of data, where the key
	 * is a unique key, and the value represents the text to be
	 * indexed by the suffix array. The delimiter 
	 * should not appear anywhere in the text strings.
	 * 
	 * @param data Map of key/text pairs
	 * @param delimiter Delimiter set between text strings
	 */
	public SuffixArraySearch2(Map<T, String> data, String delimiter) {
		text = new ArrayList<>(data.size());
		keys = new ArrayList<>(data.size());
		
		for (Entry<T, String> entry: data.entrySet()) {
			text.add(entry.getValue());
			keys.add(entry.getKey());
		}
		this.indices = new int[data.size()];
		this.suffix = new SuffixArrayX(createInputString(delimiter));
	}

    /**
     * Return a list of exact string matches for the query string.
     * This list consists of tuples, the first element is the index
     * of the string that matched, and the second element is the position
     * of the match within this string.
     * @param query
     * @return
     */
	public List<Match<T>> search(String query) {
		List<Match<T>> matches = new ArrayList<>();

		int rank = suffix.rank(query);
		// System.out.println("rank: " + rank + ": " + fragment);

		for (int i = rank; i < suffix.length(); i++) {
			if (suffix.select(i).startsWith(query)) {
				int index = Arrays.binarySearch(indices, suffix.index(i));
				// a positive index is return for an exact match. For
				// matches between two indices, a negative number is
				// returned. This negative number can be converted into
				// the index for the nearest index < input value
				if (index < 0) index = -index - 2;
				// calculate the position of the match within a string
				int position = suffix.index(i) - indices[index];
				matches.add(new Match<T>(keys.get(index), index, position));
			} else {
				break;
			}
		}
		return matches;
	}
	
	public int getSuffixArrayLength() {
		return this.suffix == null ? 0 : suffix.length();
	}
	
	private String createInputString(String delimiter) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < text.size(); i++) {
			indices[i] = sb.length();
			sb.append(text.get(i));
			sb.append(delimiter);
		}
		return sb.toString();
	}
	
	public static class Match<T> {
		private T key;
		private int index;
		private int position;
		
		public Match(T key, int index, int position) {
			this.key = key;
			this.index = index;
			this.position = position;
		}
		
		public T getKey() {
			return key;
		}
		
		public int getIndex() {
			return index;
		}
		
		public int getPosition() {
			return position;
		}	
	}
}
