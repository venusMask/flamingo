package org.apache.flamingo.utils;

import lombok.Getter;

import java.util.Objects;

@Getter
public class Pair<f0, f1> {

	private final f0 f0;

	private final f1 f1;

	public Pair(f0 f0, f1 f1) {
		this.f0 = f0;
		this.f1 = f1;
	}

	public static <f0, f1> Pair<f0, f1> of(f0 f0, f1 f1) {
		return new Pair<>(f0, f1);
	}

	@Override
	public String toString() {
		return "Pair{" + "f0=" + f0 + ", f1=" + f1 + '}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		Pair<?, ?> pair = (Pair<?, ?>) o;
		return Objects.equals(f0, pair.f0) && Objects.equals(f1, pair.f1);
	}

	@Override
	public int hashCode() {
		return Objects.hash(f0, f1);
	}

}
