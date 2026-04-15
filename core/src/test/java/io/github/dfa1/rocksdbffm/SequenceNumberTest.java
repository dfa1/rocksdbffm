package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SequenceNumberTest {

	@Test
	void of_storesExactValue() {
		// Given
		var sut = SequenceNumber.of(42);

		// When
		var result = sut.toLong();

		// Then
		assertThat(result).isEqualTo(42);
	}

	@Test
	void of_acceptsZero() {
		// Given
		var sut = SequenceNumber.of(0);

		// When
		var result = sut.toLong();

		// Then
		assertThat(result).isZero();
	}

	@Test
	void of_rejectsNegativeValues() {
		// Given / When / Then
		assertThatThrownBy(() -> SequenceNumber.of(-1)).isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	void isAfter_returnsTrueWhenStrictlyGreater() {
		// Given
		var earlier = SequenceNumber.of(1);
		var later = SequenceNumber.of(2);

		// When
		var result = later.isAfter(earlier);

		// Then
		assertThat(result).isTrue();
	}

	@Test
	void isAfter_returnsFalseWhenEqual() {
		// Given
		var a = SequenceNumber.of(5);
		var b = SequenceNumber.of(5);

		// When
		var result = a.isAfter(b);

		// Then
		assertThat(result).isFalse();
	}

	@Test
	void isAfter_returnsFalseWhenSmaller() {
		// Given
		var earlier = SequenceNumber.of(1);
		var later = SequenceNumber.of(2);

		// When
		var result = earlier.isAfter(later);

		// Then
		assertThat(result).isFalse();
	}

	@Test
	void isBefore_returnsTrueWhenStrictlySmaller() {
		// Given
		var earlier = SequenceNumber.of(1);
		var later = SequenceNumber.of(2);

		// When
		var result = earlier.isBefore(later);

		// Then
		assertThat(result).isTrue();
	}

	@Test
	void isBefore_returnsFalseWhenEqual() {
		// Given
		var a = SequenceNumber.of(5);
		var b = SequenceNumber.of(5);

		// When
		var result = a.isBefore(b);

		// Then
		assertThat(result).isFalse();
	}

	@Test
	void isBefore_returnsFalseWhenGreater() {
		// Given
		var earlier = SequenceNumber.of(1);
		var later = SequenceNumber.of(2);

		// When
		var result = later.isBefore(earlier);

		// Then
		assertThat(result).isFalse();
	}

	@Test
	void compareTo_ordersCorrectly() {
		// Given
		var a = SequenceNumber.of(1);
		var b = SequenceNumber.of(2);
		var c = SequenceNumber.of(2);

		// When
		var abOrder = a.compareTo(b);
		var baOrder = b.compareTo(a);
		var bcOrder = b.compareTo(c);

		// Then
		assertThat(abOrder).isNegative();
		assertThat(baOrder).isPositive();
		assertThat(bcOrder).isZero();
	}

	@Test
	void equals_isByValueExact() {
		// Given / When / Then
		assertThat(SequenceNumber.of(10)).isEqualTo(SequenceNumber.of(10));
		assertThat(SequenceNumber.of(10)).isNotEqualTo(SequenceNumber.of(11));
	}

	@Test
	void equals_returnsFalseForNonSequenceNumber() {
		// Given
		var sut = SequenceNumber.of(1);

		// When
		var result = sut.equals("not a sequence number");

		// Then
		assertThat(result).isFalse();
	}

	@Test
	void hashCode_isConsistentWithEquals() {
		// Given
		var a = SequenceNumber.of(99);
		var b = SequenceNumber.of(99);

		// When
		var hashA = a.hashCode();
		var hashB = b.hashCode();

		// Then
		assertThat(a).isEqualTo(b);
		assertThat(hashA).isEqualTo(hashB);
	}

	@Test
	void toString_includesValue() {
		// Given
		var sut = SequenceNumber.of(7);

		// When
		var result = sut.toString();

		// Then
		assertThat(result).isEqualTo("SequenceNumber(7)");
	}

	// --- uint64 boundary conditions ---

	@Test
	void of_acceptsLongMaxValue() {
		// Given
		var sut = SequenceNumber.of(Long.MAX_VALUE);

		// When
		var result = sut.toLong();

		// Then
		assertThat(result).isEqualTo(Long.MAX_VALUE);
	}

	@Test
	void of_rejectsValuesAboveLongMaxValue() {
		// Given
		// Long.MIN_VALUE is the Java representation of 2^63, which is a valid uint64
		// but cannot be expressed as a non-negative signed long — the constructor
		// must reject it to avoid silently misrepresenting the value.

		// When / Then
		assertThatThrownBy(() -> SequenceNumber.of(Long.MIN_VALUE))
				.isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	void compareTo_handlesUnsignedOrderingNearLongMaxValue() {
		// Given
		var belowBoundary = SequenceNumber.of(Long.MAX_VALUE - 1);
		var atBoundary = SequenceNumber.of(Long.MAX_VALUE);

		// When
		var result = belowBoundary.compareTo(atBoundary);

		// Then
		assertThat(result).isNegative();
	}

	@Test
	void isAfter_atLongMaxValue() {
		// Given
		var high = SequenceNumber.of(Long.MAX_VALUE);
		var low = SequenceNumber.of(Long.MAX_VALUE - 1);

		// When
		var highAfterLow = high.isAfter(low);
		var lowAfterHigh = low.isAfter(high);

		// Then
		assertThat(highAfterLow).isTrue();
		assertThat(lowAfterHigh).isFalse();
	}

	@Test
	void isBefore_atLongMaxValue() {
		// Given
		var high = SequenceNumber.of(Long.MAX_VALUE);
		var low = SequenceNumber.of(Long.MAX_VALUE - 1);

		// When
		var lowBeforeHigh = low.isBefore(high);
		var highBeforeLow = high.isBefore(low);

		// Then
		assertThat(lowBeforeHigh).isTrue();
		assertThat(highBeforeLow).isFalse();
	}
}
