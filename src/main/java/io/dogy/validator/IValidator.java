package io.dogy.validator;

public interface IValidator<T> {

    boolean validate(T obj);

}
