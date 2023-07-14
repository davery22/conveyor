package io.avery.pipeline;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Function;

public final class Result<T> {
    private final boolean isErr;
    private final Object val;
    
    public static void main(String[] args) {
        List.of("LICENSE", "README.md", "Unchecked.java").stream()
            .map(file -> Result.of(() -> file + ": " + Files.lines(Paths.get(file)).count())
                .expect("should count file lines"))
            .toList();
    }
    
    private Result(boolean isErr, Object val) {
        this.isErr = isErr;
        this.val = val;
    }
    
    public static <T> Result<T> ok(T ok) {
        return new Result<>(false, ok);
    }
    
    public static <T> Result<T> err(Exception err) {
        return new Result<>(true, err);
    }
    
    public static <T> Result<T> of(Callable<T> callable) {
        try {
            return ok(callable.call());
        } catch (Exception e) {
            return err(e);
        }
    }
    
    @SuppressWarnings("unchecked")
    public T unwrap() {
        if (isErr) {
            throw new AssertionError(val);
        }
        return (T) val;
    }
    
    @SuppressWarnings("unchecked")
    public T expect(String message) {
        if (isErr) {
            throw new AssertionError(message, (Exception) val);
        }
        return (T) val;
    }
    
    @SuppressWarnings("unchecked")
    public <X extends Throwable> T unwrap(Function<Exception, X> mapErr) throws X {
        if (isErr) {
            throw mapErr.apply((Exception) val);
        }
        return (T) val;
    }
}
