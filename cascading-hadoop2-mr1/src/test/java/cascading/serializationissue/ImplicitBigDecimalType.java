package cascading.serializationissue;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;

import cascading.CascadingException;
import cascading.tuple.type.CoercibleType;
import cascading.util.Util;

public class ImplicitBigDecimalType implements CoercibleType<BigDecimal> {

	private static final long serialVersionUID = 6712110015613181562L;
	private int scale;
	private boolean isImplicit = false;

	public ImplicitBigDecimalType(int scale) {
		this.scale = scale;
	}

	public ImplicitBigDecimalType(int scale, boolean isImplicit) {
		this.scale = scale;
		this.isImplicit = isImplicit;
	}

	@Override
	public BigDecimal canonical(Object value) {
		if (value == null)
			return null;

		
		Class from = value.getClass();

		if (this.isImplicit) {
			if (from == BigDecimal.class)
				return ((BigDecimal) value).movePointLeft(scale)
						.setScale(scale);
			if (from == Double.class)
				return BigDecimal.valueOf((Double) value).movePointLeft(scale)
						.setScale(scale);
			if (from == BigInteger.class)
				return new BigDecimal((BigInteger) value).movePointLeft(scale)
						.setScale(scale);
			if (from == Long.class)
				return BigDecimal.valueOf((Long) value).movePointLeft(scale)
						.setScale(scale);
			if (from == String.class)
				return new BigDecimal(value.toString().trim()).movePointLeft(scale)
						.setScale(scale);
		} else {
			if (from == BigDecimal.class)
				return ((BigDecimal) value).setScale(scale);
			if (from == Double.class)
				return BigDecimal.valueOf((Double) value).setScale(scale);
			if (from == BigInteger.class)
				return new BigDecimal((BigInteger) value).setScale(scale);
			if (from == Long.class)
				return BigDecimal.valueOf((Long) value).setScale(scale);
			
			if (from == String.class) {
				if(value.toString().trim().equals("")) //BigDecimal throws NumberFormatException for blank string
					return null;
				return new BigDecimal(value.toString().trim()).setScale(scale);
			}
				
		}

		throw new CascadingException("unknown type coercion requested from: "
				+ Util.getTypeName(from));
	}

	@Override
	public Object coerce(Object value, Type type) {
		if (value == null)
			return null;

		Class from = value.getClass();

		if (this.isImplicit) {
			if (from == BigDecimal.class)
				return ((BigDecimal) value).movePointLeft(scale)
						.setScale(scale);
			if (from == Double.class)
				return BigDecimal.valueOf((Double) value).movePointLeft(scale)
						.setScale(scale);
			if (from == BigInteger.class)
				return new BigDecimal((BigInteger) value).movePointLeft(scale)
						.setScale(scale);
			if (from == Long.class)
				return BigDecimal.valueOf((Long) value).movePointLeft(scale)
						.setScale(scale);
			if (from == String.class)
				return new BigDecimal(value.toString().trim()).movePointLeft(scale)
						.setScale(scale);
		} else {
			if (from == BigDecimal.class)
				return ((BigDecimal) value).setScale(scale);
			if (from == Double.class)
				return BigDecimal.valueOf((Double) value).setScale(scale);
			if (from == BigInteger.class)
				return new BigDecimal((BigInteger) value).setScale(scale);
			if (from == Long.class)
				return BigDecimal.valueOf((Long) value).setScale(scale);
			if (from == String.class){
				if(value.toString().trim().equals("")) //BigDecimal throws NumberFormatException for blank string
					return null;
				return new BigDecimal(value.toString().trim()).setScale(scale);
			}
		}

		throw new CascadingException("unknown type coercion requested from: "
				+ Util.getTypeName(from));
	}

	@Override
	public Class getCanonicalType() {
		return ImplicitBigDecimalType.class;
	}

}