package com.techpago.validator;

public interface IValidator<T> {

    boolean validate(T obj);

}
