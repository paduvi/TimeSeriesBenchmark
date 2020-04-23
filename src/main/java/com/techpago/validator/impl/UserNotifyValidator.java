package com.techpago.validator.impl;

import com.techpago.model.UserNotify;
import com.techpago.validator.IValidator;
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
