package io.dogy.validator.impl;

import io.dogy.model.UserNotify;
import io.dogy.validator.IValidator;
import org.springframework.stereotype.Component;

@Component
public class UserNotifyValidator implements IValidator<UserNotify> {

    @Override
    public boolean validate(UserNotify userNotify) {
        if (userNotify.getUserID() == null) {
            return false;
        }
        if (userNotify.getNotifyID() == null) {
            return false;
        }
        return userNotify.getData() != null;
    }

}
